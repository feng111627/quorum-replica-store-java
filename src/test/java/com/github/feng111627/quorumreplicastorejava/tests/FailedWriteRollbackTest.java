package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.EntryState;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.Status;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import com.github.feng111627.quorumreplicastorejava.node.StoredEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 业务场景：写失败干净收场（未凑够确认的写当作从没发生）。
 * 验证点：失败写不可读、不覆盖旧值、节点重上线/整机重启不复活、不被读修复传播、
 * 重复提交同一失败 opId 也不复活；同时成功写、顺序更新等正常行为不受影响。
 * 集群：5 节点、3 分片、每分片 3 副本，W=2、R=2。
 */
class FailedWriteRollbackTest {
    private static final Logger log = LoggerFactory.getLogger(FailedWriteRollbackTest.class);
    private static final List<String> NODES = List.of("n1", "n2", "n3", "n4", "n5");

    @TempDir
    Path dataDir;

    NodeRegistry registry;
    Coordinator coordinator;

    @BeforeEach
    void setUp() {
        // 启用协调器决定日志（dataDir 落盘），覆盖整机重启后的决定恢复
        registry = new NodeRegistry(NODES, 3, 3, dataDir);
        coordinator = new Coordinator(registry, 2, 2, dataDir);
        log.info("[业务:失败写回滚] 初始化集群 W=2 R=2 dataDir={}", dataDir);
    }

    private List<String> replicas(String key) {
        return registry.router().replicasOf(key);
    }

    @Test
    void failedWriteToNewKeyIsNeverReadableAndLeavesNoResidue() {
        String key = "rollback-new";
        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down(); // 只剩 1 个可写，W=2 达不到

        WriteResult w = coordinator.put("fail-1", key, "ghost", null);
        log.info("[业务:失败写回滚][用例:新键失败写不可读] write={} message={} 期望=WRITE_QUORUM_NOT_MET",
                w.status(), w.message());
        assertEquals(Status.WRITE_QUORUM_NOT_MET, w.status());

        // 恢复全部节点后读：新键就是查不到，而不是 CONFLICT 或读到 ghost
        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();
        ReadResult g = coordinator.get(key);
        log.info("[业务:失败写回滚][用例:新键失败写不可读] read={} values={} 期望=NOT_FOUND",
                g.status(), g.values().size());
        assertEquals(Status.NOT_FOUND, g.status());
        assertTrue(g.values().isEmpty(), "未凑够确认的值绝不允许被读到");

        // 唯一收到预备值的副本上，该版本应是 ABORTED 状态，绝不 COMMITTED
        List<StoredEntry> entries = registry.node(r.get(2)).localEntries(key);
        log.info("[业务:失败写回滚][用例:残留版本状态为已放弃] 节点={} entries={}", r.get(2),
                entries.stream().map(e -> e.state() + ":" + e.version().value()).toList());
        assertTrue(entries.stream().allMatch(e -> e.state() == EntryState.ABORTED));
        assertTrue(entries.stream().noneMatch(e -> e.state() == EntryState.COMMITTED));
    }

    @Test
    void failedWriteKeepsPreviousCommittedValue() {
        String key = "rollback-existing";
        WriteResult base = coordinator.put("ok-1", key, "before", null);
        assertEquals(Status.OK, base.status());

        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        WriteResult w = coordinator.put("fail-2", key, "after-ghost", null);
        log.info("[业务:失败写回滚][用例:失败写不覆盖旧值] write={} 期望=WRITE_QUORUM_NOT_MET", w.status());
        assertEquals(Status.WRITE_QUORUM_NOT_MET, w.status());

        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();
        ReadResult g = coordinator.get(key);
        log.info("[业务:失败写回滚][用例:失败写不覆盖旧值] read={} value={} 期望=OK/before",
                g.status(), g.values().isEmpty() ? null : g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("before", g.values().get(0).value(), "同一键必须读到失败写之前的状态");
    }

    @Test
    void failedWriteDoesNotResurrectAfterReplicaRestart() {
        String key = "rollback-restart-node";
        List<String> r = replicas(key);
        // 只留一个副本在线接收预备值
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        assertEquals(Status.WRITE_QUORUM_NOT_MET,
                coordinator.put("fail-3", key, "ghost", null).status());

        // 承载了预备值的节点宕机再重启（重放 WAL）：放弃决定也必须重放，ghost 不得复活
        ReplicaNode carrier = registry.node(r.get(2));
        carrier.down();
        carrier.up();
        // 其余两个副本恢复，凑齐 R=2 后读
        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();
        ReadResult g = coordinator.get(key);
        log.info("[业务:失败写回滚][用例:承载节点重启不复活] read={} 期望=NOT_FOUND", g.status());
        assertEquals(Status.NOT_FOUND, g.status());

        List<StoredEntry> entries = carrier.localEntries(key);
        log.info("[业务:失败写回滚][用例:承载节点重启不复活] 重放后 entries={}",
                entries.stream().map(e -> e.state() + ":" + e.version().value()).toList());
        assertTrue(entries.stream().noneMatch(e -> e.state() == EntryState.COMMITTED),
                "重启重放 WAL 后残留仍不得成为可读数据");
    }

    @Test
    void failedWriteDoesNotResurrectAfterFullClusterRestart() {
        String key = "rollback-full-restart";
        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        assertEquals(Status.WRITE_QUORUM_NOT_MET,
                coordinator.put("fail-4", key, "ghost", null).status());

        // 整机重启：用同一数据目录重建注册表与协调器（重放节点 WAL + 协调器决定日志）
        registry.all().values().forEach(ReplicaNode::down);
        NodeRegistry registry2 = new NodeRegistry(NODES, 3, 3, dataDir);
        Coordinator coordinator2 = new Coordinator(registry2, 2, 2, dataDir);

        ReadResult g = coordinator2.get(key);
        log.info("[业务:失败写回滚][用例:整机重启不复活] read={} values={} 期望=NOT_FOUND",
                g.status(), g.values().size());
        assertEquals(Status.NOT_FOUND, g.status());
    }

    @Test
    void retryingSameFailedOpIdDoesNotCommitGhost() {
        String key = "rollback-retry";
        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();

        WriteResult w1 = coordinator.put("fail-dup", key, "ghost", null);
        WriteResult w2 = coordinator.put("fail-dup", key, "ghost", null); // 同 opId 重试
        log.info("[业务:失败写回滚][用例:失败opId重试不复活] first={} second={} 期望=都失败",
                w1.status(), w2.status());
        assertEquals(Status.WRITE_QUORUM_NOT_MET, w1.status());
        assertEquals(Status.WRITE_QUORUM_NOT_MET, w2.status());

        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();
        ReadResult g = coordinator.get(key);
        assertEquals(Status.NOT_FOUND, g.status());
        log.info("[业务:失败写回滚][用例:失败opId重试不复活] 恢复后 read={} 期望=NOT_FOUND", g.status());
    }

    @Test
    void successfulWritesStillWorkAfterRollbackFeature() {
        // 正常成功写 + 基于读上下文的顺序更新不被两阶段改动破坏
        WriteResult w1 = coordinator.put("ok-a", "rollback-normal", "v1", null);
        assertEquals(Status.OK, w1.status());
        ReadResult g1 = coordinator.get("rollback-normal");
        assertEquals(Status.OK, g1.status());
        assertEquals("v1", g1.values().get(0).value());

        WriteResult w2 = coordinator.put("ok-b", "rollback-normal", "v2", g1.context());
        ReadResult g2 = coordinator.get("rollback-normal");
        log.info("[业务:失败写回滚][用例:成功写与顺序更新不受影响] write2={} read={} value={}",
                w2.status(), g2.status(), g2.values().get(0).value());
        assertEquals(Status.OK, w2.status());
        assertEquals(Status.OK, g2.status());
        assertEquals("v2", g2.values().get(0).value());
        assertEquals(1, g2.values().size());
    }

    @Test
    void laterSuccessfulWriteToSameKeyIsNotBlockedByAbortedResidue() {        String key = "rollback-then-ok";
        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        assertEquals(Status.WRITE_QUORUM_NOT_MET,
                coordinator.put("fail-5", key, "ghost", null).status());

        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();
        WriteResult ok = coordinator.put("ok-6", key, "real", null);
        ReadResult g = coordinator.get(key);
        log.info("[业务:失败写回滚][用例:放弃残留不阻塞后续成功写] write={} read={} value={}",
                ok.status(), g.status(), g.values().get(0).value());
        assertEquals(Status.OK, ok.status());
        assertEquals(Status.OK, g.status());
        assertEquals("real", g.values().get(0).value());
    }

    @Test
    void failedDeleteDoesNotHidePreviouslyCommittedValue() {
        String key = "rollback-delete";
        assertEquals(Status.OK, coordinator.put("ok-1", key, "keep", null).status());
        ReadResult r = coordinator.get(key);

        List<String> rr = replicas(key);
        registry.node(rr.get(0)).down();
        registry.node(rr.get(1)).down();
        // 删除写凑不够确认：tombstone 预备值必须被放弃，旧值仍然可读
        WriteResult d = coordinator.delete("fail-del", key, r.context());
        log.info("[业务:失败写回滚][用例:失败删除不隐藏旧值] delete={} 期望=WRITE_QUORUM_NOT_MET", d.status());
        assertEquals(Status.WRITE_QUORUM_NOT_MET, d.status());

        registry.node(rr.get(0)).up();
        registry.node(rr.get(1)).up();
        ReadResult g = coordinator.get(key);
        log.info("[业务:失败写回滚][用例:失败删除不隐藏旧值] read={} value={} 期望=OK/keep",
                g.status(), g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("keep", g.values().get(0).value());
    }

    @Test
    void committedValuePropagatesToReplicaThatWasDownViaReadRepair() {
        String key = "rollback-missed-prepare";
        List<String> r = replicas(key);
        // 提交期间第三个副本下线：W=2 仍成功；它重新上线后，读修复/收敛应把完整已提交值带给它，
        // 且该值是带值的已提交版本，而不是时钟-only 的空壳
        ReplicaNode missed = registry.node(r.get(2));
        missed.down();
        WriteResult w = coordinator.put("ok-1", key, "spread", null);
        log.info("[业务:失败写回滚][用例:下线副本经读修复补齐] write={} 期望=OK", w.status());
        assertEquals(Status.OK, w.status());
        assertEquals(0, missed.localCommittedGet(key) == null ? 0 : missed.localCommittedGet(key).size());

        missed.up(); // 节点在提交结束后才上线
        ReadResult g = coordinator.get(key); // 读修复把已提交值传播给它
        log.info("[业务:失败写回滚][用例:下线副本经读修复补齐] read={} value={}",
                g.status(), g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("spread", g.values().get(0).value());

        List<com.github.feng111627.quorumreplicastorejava.core.VersionedValue> local =
                missed.localCommittedGet(key);
        assertEquals(1, local.size());
        assertEquals("spread", local.get(0).value(), "补齐的必须是带值的完整已提交版本");
        assertTrue(coordinator.inspectKey(key).consistent());
    }
}
