package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.Status;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 业务场景：节点故障与重启恢复。
 * 集群：5 节点、3 分片、每分片 3 副本，W=2、R=2。
 */
class RecoveryTest {
    private static final Logger log = LoggerFactory.getLogger(RecoveryTest.class);
    private static final List<String> NODES = List.of("n1", "n2", "n3", "n4", "n5");

    @TempDir
    Path dataDir;

    NodeRegistry registry;
    Coordinator coordinator;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry(NODES, 3, 3, dataDir);
        coordinator = new Coordinator(registry, 2, 2);
        log.info("[业务:故障与恢复] 初始化集群 W=2 R=2");
    }

    @Test
    void nodeRestartRecoversCommittedStateFromWal() {
        coordinator.put("op-1", "rk1", "committed-value", null);
        String replicaId = registry.router().replicasOf("rk1").get(0);
        ReplicaNode node = registry.node(replicaId);

        node.down(); // 模拟宕机：内存丢失
        assertTrue(node.localGet("rk1") == null, "下线节点不应响应");
        node.up();   // 模拟重启：从 WAL 恢复

        List<VersionedValue> local = node.localGet("rk1");
        log.info("[业务:故障与恢复][用例:重启恢复已提交状态] 节点={} 恢复版本数={} value={} 期望=1/committed-value",
                replicaId, local.size(), local.isEmpty() ? null : local.get(0).value());
        assertEquals(1, local.size());
        assertEquals("committed-value", local.get(0).value());
    }

    @Test
    void fullClusterRestartPreservesCommittedData() {
        coordinator.put("op-1", "rk2", "durable", null);

        // 全部节点宕机后，用同一数据目录重建整个集群（模拟整机重启）
        registry.all().values().forEach(ReplicaNode::down);
        NodeRegistry registry2 = new NodeRegistry(NODES, 3, 3, dataDir);
        Coordinator coordinator2 = new Coordinator(registry2, 2, 2);

        ReadResult g = coordinator2.get("rk2");
        log.info("[业务:故障与恢复][用例:全集群重启] read={} value={} 期望=OK/durable",
                g.status(), g.values().isEmpty() ? null : g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("durable", g.values().get(0).value());
    }

    @Test
    void duplicateSubmissionOfSameOpIsIdempotent() {
        WriteResult w1 = coordinator.put("op-dup", "rk3", "v", null);
        WriteResult w2 = coordinator.put("op-dup", "rk3", "v", null); // 同一 opId 重复提交
        log.info("[业务:故障与恢复][用例:重复提交幂等] first={} second={} 期望=OK/OK", w1.status(), w2.status());
        assertEquals(Status.OK, w1.status());
        assertEquals(Status.OK, w2.status());

        ReadResult g = coordinator.get("rk3");
        log.info("[业务:故障与恢复][用例:重复提交幂等] read={} 版本数={} 期望=OK/1（无重复版本）",
                g.status(), g.values().size());
        assertEquals(Status.OK, g.status());
        assertEquals(1, g.values().size(), "重复提交不得产生重复版本");
    }

    @Test
    void walReplayIsIdempotentAcrossMultipleRestarts() {
        coordinator.put("op-1", "rk4", "v", null);
        String replicaId = registry.router().replicasOf("rk4").get(0);
        ReplicaNode node = registry.node(replicaId);

        node.down();
        node.up();
        node.down();
        node.up(); // 重放两次 WAL

        List<VersionedValue> local = node.localGet("rk4");
        log.info("[业务:故障与恢复][用例:WAL重放幂等] 节点={} 两次重启后版本数={} 期望=1（无版本错乱）",
                replicaId, local.size());
        assertEquals(1, local.size());
        assertEquals("v", local.get(0).value());
    }

    @Test
    void staleReplicaRejoiningDoesNotRollBackCommittedData() {
        // 关掉一个副本，提交新值（W=2 仍满足），再把它拉回来：读应归并出新值并修复它
        String key = "rk5";
        List<String> replicas = registry.router().replicasOf(key);
        ReplicaNode staleNode = registry.node(replicas.get(0));
        staleNode.down();
        log.info("[业务:故障与恢复][用例:过期副本重新加入] 副本 {} 下线期间提交新值", replicas.get(0));

        WriteResult w = coordinator.put("op-1", key, "new-committed", null);
        assertEquals(Status.OK, w.status());

        staleNode.up(); // 重新加入，它本地没有这个键（或只有旧值）
        ReadResult g = coordinator.get(key); // 读触发归并 + 读修复
        log.info("[业务:故障与恢复][用例:过期副本重新加入] read={} value={} 期望=OK/new-committed（不回退）",
                g.status(), g.values().isEmpty() ? null : g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("new-committed", g.values().get(0).value());

        // 验证读修复：重新加入的副本现在也持有了已提交的新值
        List<VersionedValue> local = staleNode.localGet(key);
        log.info("[业务:故障与恢复][用例:过期副本重新加入] 读修复后副本 {} 本地版本数={} value={}",
                replicas.get(0), local.size(), local.isEmpty() ? null : local.get(0).value());
        assertEquals(1, local.size());
        assertEquals("new-committed", local.get(0).value());
    }

    @Test
    void writeDuringPartitionSucceedsOnMajoritySideAndHealsAfterRejoin() {
        // 模拟分区：1 个副本被隔离，写入在多数侧成功；分区恢复后读取一致
        String key = "rk6";
        List<String> replicas = registry.router().replicasOf(key);
        ReplicaNode isolated = registry.node(replicas.get(2));
        isolated.down();

        WriteResult w = coordinator.put("op-1", key, "majority-value", null);
        log.info("[业务:故障与恢复][用例:分区写入与愈合] 分区期间写={} 期望=OK（多数侧满足W=2）", w.status());
        assertEquals(Status.OK, w.status());

        isolated.up();
        ReadResult g = coordinator.get(key);
        log.info("[业务:故障与恢复][用例:分区写入与愈合] 愈合后读={} value={}",
                g.status(), g.values().isEmpty() ? null : g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("majority-value", g.values().get(0).value());
    }
}
