package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.model.OpStatus;
import com.github.feng111627.quorumreplicastorejava.model.ReadResult;
import com.github.feng111627.quorumreplicastorejava.model.Version;
import com.github.feng111627.quorumreplicastorejava.model.VersionedValue;
import com.github.feng111627.quorumreplicastorejava.model.WriteResult;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import com.github.feng111627.quorumreplicastorejava.service.CoordinatorService;
import com.github.feng111627.quorumreplicastorejava.shard.ShardLayout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分布式 KV 存储业务场景测试：
 *  1. 法定人数读写（正常路径）
 *  2. 写法定人数不足 → WRITE_QUORUM_NOT_MET
 *  3. 读法定人数不足 → READ_QUORUM_NOT_MET
 *  4. 并发写冲突 → CONFLICT
 *  5. 过期副本数据不得覆盖更新的已提交结果
 *  6. 节点宕机重启后恢复到崩溃前已提交状态
 *  7. 重复提交/重放幂等，不产生重复或版本错乱
 *  8. 恢复后读取不出现已提交数据回退
 */
class QuorumStoreTests {

    private static final Logger log = LoggerFactory.getLogger(QuorumStoreTests.class);

    private static final int SHARDS = 3;
    private static final int REPLICAS = 3;
    private static final int W = 2;
    private static final int R = 2;

    @TempDir
    Path dataDir;

    private ShardLayout layout;
    private CoordinatorService coordinator;

    @BeforeEach
    void setUp() {
        List<List<ReplicaNode>> groups = new ArrayList<>();
        for (int s = 0; s < SHARDS; s++) {
            List<ReplicaNode> group = new ArrayList<>();
            for (int r = 0; r < REPLICAS; r++) {
                group.add(new ReplicaNode("shard" + s + "-node" + r, dataDir));
            }
            groups.add(group);
        }
        layout = new ShardLayout(SHARDS, groups);
        coordinator = new CoordinatorService(layout, W, R);
    }

    private List<ReplicaNode> replicasOf(String key) {
        return layout.replicasFor(key);
    }

    @Test
    void quorumReadWriteHappyPath() {
        log.info("[用例] 法定人数读写-正常路径: W={}, R={}", W, R);
        WriteResult w = coordinator.write("user:1", "alice", null, "op-1");
        assertEquals(OpStatus.OK, w.status());
        ReadResult r = coordinator.read("user:1");
        assertEquals(OpStatus.OK, r.status());
        assertEquals("alice", r.value().value());
        log.info("[结果] 写入版本 {}，读回值 '{}' ✔", w.version(), r.value().value());
    }

    @Test
    void writeQuorumNotMetWhenTooManyReplicasDown() {
        log.info("[用例] 写法定人数不足: 下线 2/3 副本，W={}", W);
        List<ReplicaNode> replicas = replicasOf("user:2");
        replicas.get(0).setUp(false);
        replicas.get(1).setUp(false);
        WriteResult w = coordinator.write("user:2", "bob", null, "op-2");
        assertEquals(OpStatus.WRITE_QUORUM_NOT_MET, w.status());
        log.info("[结果] 返回 {}，消息: {} ✔", w.status(), w.message());
    }

    @Test
    void readQuorumNotMetWhenTooManyReplicasDown() {
        log.info("[用例] 读法定人数不足: 先写入再下线 2/3 副本，R={}", R);
        WriteResult w = coordinator.write("user:3", "carol", null, "op-3");
        assertEquals(OpStatus.OK, w.status());
        List<ReplicaNode> replicas = replicasOf("user:3");
        replicas.get(0).setUp(false);
        replicas.get(1).setUp(false);
        ReadResult r = coordinator.read("user:3");
        assertEquals(OpStatus.READ_QUORUM_NOT_MET, r.status());
        log.info("[结果] 返回 {}，未悄悄返回旧值 ✔", r.status());
    }

    @Test
    void concurrentWriteConflictDetected() {
        log.info("[用例] 并发写冲突: 两个客户端基于同一版本写入");
        WriteResult first = coordinator.write("user:4", "v1", null, "op-4a");
        assertEquals(OpStatus.OK, first.status());
        Version base = first.version();
        // 两个并发客户端都基于 base 写
        WriteResult c1 = coordinator.write("user:4", "client-A", base, "op-4b");
        WriteResult c2 = coordinator.write("user:4", "client-B", base, "op-4c");
        assertEquals(OpStatus.OK, c1.status());
        assertEquals(OpStatus.CONFLICT, c2.status());
        ReadResult r = coordinator.read("user:4");
        assertEquals("client-A", r.value().value());
        log.info("[结果] 后到的写入返回 CONFLICT，已提交值未被覆盖: '{}' ✔", r.value().value());
    }

    @Test
    void staleReplicaCannotOverwriteCommittedData() {
        log.info("[用例] 过期副本数据不得覆盖更新的已提交结果");
        coordinator.write("user:5", "new", null, "op-5");
        ReplicaNode node = replicasOf("user:5").get(0);
        VersionedValue committed = node.get("user:5").orElseThrow();
        VersionedValue stale = new VersionedValue("old",
                new Version(committed.version().sequence() - 1 + 1, "a"), "op-stale");
        // 构造一个比已提交版本更旧的写入
        VersionedValue older = new VersionedValue("old",
                new Version(0, "z"), "op-stale");
        assertFalse(node.put("user:5", older));
        assertEquals("new", node.get("user:5").orElseThrow().value());
        log.info("[结果] 节点拒绝过期版本，保留已提交值 'new' ✔ (stale={})", stale.version());
    }

    @Test
    void failedWriteLeavesNoTrace() {
        log.info("[用例] 写法定人数不足回滚: 失败的写不得留下痕迹");
        List<ReplicaNode> replicas = replicasOf("user:9");
        replicas.get(0).setUp(false);
        replicas.get(1).setUp(false);
        WriteResult w = coordinator.write("user:9", "ghost", null, "op-9");
        assertEquals(OpStatus.WRITE_QUORUM_NOT_MET, w.status());
        // 已确认过的副本上应被回滚干净
        assertTrue(replicas.get(2).get("user:9").isEmpty());
        // 节点恢复、集群满员后读取：不得出现那次失败的写入
        replicas.get(0).setUp(true);
        replicas.get(1).setUp(true);
        ReadResult r = coordinator.read("user:9");
        assertEquals(OpStatus.KEY_NOT_FOUND, r.status());
        log.info("[结果] 失败写入已回滚，恢复后读取返回 KEY_NOT_FOUND ✔");
    }

    @Test
    void nodeRestartRecoversCommittedState() {
        log.info("[用例] 节点宕机重启恢复: 崩溃后从 WAL 恢复已提交状态");
        coordinator.write("user:6", "dave", null, "op-6");
        ReplicaNode node = replicasOf("user:6").get(0);
        node.setUp(false);          // 模拟宕机
        node.recover();             // 模拟重启：从 WAL 重放
        node.setUp(true);
        assertEquals("dave", node.get("user:6").orElseThrow().value());
        ReadResult r = coordinator.read("user:6");
        assertEquals("dave", r.value().value());
        log.info("[结果] 重启后节点数据与崩溃前一致，集群读取正常 ✔");
    }

    @Test
    void duplicateSubmissionIsIdempotent() {
        log.info("[用例] 重复提交/重放幂等: 同一 opId 不产生重复或版本错乱");
        WriteResult w1 = coordinator.write("user:7", "eve", null, "op-7");
        WriteResult w2 = coordinator.write("user:7", "eve", null, "op-7"); // 重试
        assertEquals(OpStatus.OK, w2.status());
        assertEquals(w1.version(), w2.version());
        // 直接对节点重放同一操作
        ReplicaNode node = replicasOf("user:7").get(0);
        VersionedValue committed = node.get("user:7").orElseThrow();
        assertTrue(node.put("user:7", committed)); // 同 opId 重放 → 幂等成功
        assertEquals(committed, node.get("user:7").orElseThrow());
        ReadResult r = coordinator.read("user:7");
        assertEquals(w1.version(), r.value().version());
        log.info("[结果] 重试与重放均幂等，版本保持 {} ✔", w1.version());
    }

    @Test
    void noCommittedDataRegressionAfterRecovery() {
        log.info("[用例] 恢复后读取不出现已提交数据回退");
        WriteResult w1 = coordinator.write("user:8", "v1", null, "op-8a");
        WriteResult w2 = coordinator.write("user:8", "v2", w1.version(), "op-8b");
        assertEquals(OpStatus.OK, w2.status());
        // 让一个副本宕机错过第二次写入的场景：先下线再写
        List<ReplicaNode> replicas = replicasOf("user:8");
        replicas.get(2).setUp(false);
        coordinator.write("user:8", "v3", w2.version(), "op-8c");
        // 该副本带着旧数据重新上线，读修复不得让已提交数据回退
        replicas.get(2).recover();
        replicas.get(2).setUp(true);
        ReadResult r = coordinator.read("user:8");
        assertEquals("v3", r.value().value());
        // 读修复后，落后副本被推进到最新版本
        assertEquals("v3", replicas.get(2).get("user:8").orElseThrow().value());
        log.info("[结果] 读取返回最新已提交值 'v3'，落后副本已读修复 ✔");
    }
}
