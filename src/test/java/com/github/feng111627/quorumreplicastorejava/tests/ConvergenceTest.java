package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.ConvergeReport;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.Diagnosis;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.KeyConvergence;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.Status;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
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
 * 业务场景：按需收敛——把不一致的键修到一致。
 * 幂等可反复执行；绝不把未凑够法定人数的写提升为可读数据。
 * 集群：5 节点、3 分片、每分片 3 副本，W=2、R=2。
 */
class ConvergenceTest {
    private static final Logger log = LoggerFactory.getLogger(ConvergenceTest.class);
    private static final List<String> NODES = List.of("n1", "n2", "n3", "n4", "n5");

    @TempDir
    Path dataDir;

    NodeRegistry registry;
    Coordinator coordinator;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry(NODES, 3, 3, dataDir);
        coordinator = new Coordinator(registry, 2, 2);
        log.info("[业务:按需收敛] 初始化集群 W=2 R=2");
    }

    /** 在指定副本上手工种一个未提交残留（模拟 prepare 后没等到决定的遗留）。 */
    private void plantResidue(String nodeId, String opId, String key, String value) {
        VectorClock clock = new VectorClock();
        clock.increment("w:" + opId);
        ReplicaNode.WriteAck ack = registry.node(nodeId).localPut(opId, key, value, null, clock, false);
        assertTrue(ack.accepted(), "种残留应被 prepare 接受");
    }

    @Test
    void convergeHealsLaggingReplica() {
        String key = "cv:1";
        List<String> replicas = registry.router().replicasOf(key);
        ReplicaNode staleNode = registry.node(replicas.get(0));
        staleNode.down();
        coordinator.put("op-c1", key, "committed", null);
        staleNode.up();
        assertEquals(Diagnosis.LAGGING, coordinator.inspect(key).keys().get(0).diagnosis());

        KeyConvergence c = coordinator.converge(key).keys().get(0);
        log.info("[业务:按需收敛][用例:修复落后副本] repaired={} after={} 期望=[{}]/CONSISTENT",
                c.repairedNodes(), c.after(), replicas.get(0));
        assertEquals(List.of(replicas.get(0)), c.repairedNodes());
        assertEquals(Diagnosis.CONSISTENT, c.after());

        ReplicaNode.Snapshot snap = staleNode.localInspect(key);
        assertEquals(1, snap.committed().size());
        assertEquals("committed", snap.committed().get(0).value());
    }

    @Test
    void convergeRemovesResidueWithoutMakingItReadable() {
        String key = "cv:2";
        List<String> replicas = registry.router().replicasOf(key);
        plantResidue(replicas.get(1), "op-ghost-2", key, "never-committed");

        KeyConvergence c = coordinator.converge(key).keys().get(0);
        log.info("[业务:按需收敛][用例:清除残留] aborted={} after={} 期望=1条/CONSISTENT",
                c.abortedResidue(), c.after());
        assertEquals(1, c.abortedResidue().size());
        assertEquals(Diagnosis.CONSISTENT, c.after());

        ReadResult g = coordinator.get(key);
        log.info("[业务:按需收敛][用例:清除残留] read={} 期望=NOT_FOUND（残留不得变成可读数据）", g.status());
        assertEquals(Status.NOT_FOUND, g.status());
        for (String id : replicas) {
            ReplicaNode.Snapshot snap = registry.node(id).localInspect(key);
            assertEquals(0, snap.pending().size(), "残留必须被清除");
            assertEquals(0, snap.committed().size(), "残留不得被提升为已提交");
        }
    }

    @Test
    void convergeNeverPromotesUncommittedWriteEvenWhenAllReplicasHoldIt() {
        String key = "cv:3";
        List<String> replicas = registry.router().replicasOf(key);
        // 模拟协调器在 prepare 之后、commit 决定之前宕机：所有副本都只有 pending
        for (String id : replicas) {
            plantResidue(id, "op-orphan", key, "orphan-value");
        }
        assertEquals(Diagnosis.UNCOMMITTED_RESIDUE, coordinator.inspect(key).keys().get(0).diagnosis());

        KeyConvergence c = coordinator.converge(key).keys().get(0);
        log.info("[业务:按需收敛][用例:绝不提升未提交写] aborted={} after={} 期望=3条/CONSISTENT",
                c.abortedResidue(), c.after());
        assertEquals(3, c.abortedResidue().size(), "所有副本上的残留都应被清除");

        ReadResult g = coordinator.get(key);
        log.info("[业务:按需收敛][用例:绝不提升未提交写] read={} 期望=NOT_FOUND", g.status());
        assertEquals(Status.NOT_FOUND, g.status(),
                        "即使所有副本都持有同一未提交值，收敛也绝不能把它变成可读数据");
        for (String id : replicas) {
            ReplicaNode.Snapshot snap = registry.node(id).localInspect(key);
            assertEquals(0, snap.committed().size());
            assertEquals(0, snap.pending().size());
        }
    }

    @Test
    void convergeIsIdempotentAndRepeatable() {
        String key = "cv:4";
        List<String> replicas = registry.router().replicasOf(key);
        ReplicaNode staleNode = registry.node(replicas.get(0));
        staleNode.down();
        coordinator.put("op-c4", key, "v", null);
        staleNode.up();
        plantResidue(replicas.get(1), "op-ghost-4", key, "residue");

        KeyConvergence first = coordinator.converge(key).keys().get(0);
        KeyConvergence second = coordinator.converge(key).keys().get(0);
        log.info("[业务:按需收敛][用例:幂等可重复] 第一次repaired={} aborted={} after={}；第二次repaired={} aborted={} after={}",
                first.repairedNodes(), first.abortedResidue(), first.after(),
                second.repairedNodes(), second.abortedResidue(), second.after());
        assertEquals(Diagnosis.CONSISTENT, first.after());
        assertEquals(Diagnosis.CONSISTENT, second.after());
        assertTrue(second.repairedNodes().isEmpty(), "第二次收敛不应再有修复动作");
        assertTrue(second.abortedResidue().isEmpty(), "第二次收敛不应再有清除动作");

        ReadResult g = coordinator.get(key);
        assertEquals(Status.OK, g.status());
        assertEquals("v", g.values().get(0).value());
    }

    @Test
    void convergePreservesCommittedConflictSiblings() {
        String key = "cv:5";
        WriteResult w1 = coordinator.put("op-c5-a", key, "base", null);
        coordinator.put("op-c5-b", key, "B", w1.context());
        coordinator.put("op-c5-c", key, "C", w1.context());
        ReadResult before = coordinator.get(key);
        assertEquals(Status.CONFLICT, before.status());

        KeyConvergence c = coordinator.converge(key).keys().get(0);
        log.info("[业务:按需收敛][用例:保留已提交冲突] after={} 期望=CONSISTENT（兄弟集一致）", c.after());
        assertEquals(Diagnosis.CONSISTENT, c.after());

        ReadResult after = coordinator.get(key);
        log.info("[业务:按需收敛][用例:保留已提交冲突] read={} siblings={} 期望=CONFLICT/2（收敛不替客户端解决冲突）",
                after.status(), after.values().size());
        assertEquals(Status.CONFLICT, after.status(), "已提交的并发兄弟必须保留，收敛不等于冲突解决");
        assertEquals(2, after.values().size());
    }

    @Test
    void convergeAllFixesEverythingAndSurvivesRestart() {
        // 键1：落后；键2：残留；键3：一致
        List<String> replicas1 = registry.router().replicasOf("cv:6a");
        ReplicaNode stale = registry.node(replicas1.get(0));
        stale.down();
        coordinator.put("op-c6a", "cv:6a", "committed", null);
        stale.up();

        List<String> replicas2 = registry.router().replicasOf("cv:6b");
        plantResidue(replicas2.get(1), "op-ghost-6", "cv:6b", "residue");

        coordinator.put("op-c6c", "cv:6c", "fine", null);

        ConvergeReport report = coordinator.convergeAll();
        log.info("[业务:按需收敛][用例:全库收敛] summary={}", report.summary());
        for (KeyConvergence kc : report.keys()) {
            log.info("[业务:按需收敛][用例:全库收敛] key={} after={} repaired={} aborted={}",
                    kc.key(), kc.after(), kc.repairedNodes(), kc.abortedResidue());
            assertEquals(Diagnosis.CONSISTENT, kc.after());
        }

        // 全集群重启（模拟整机重启）：收敛结果持久，残留不复活
        registry.all().values().forEach(ReplicaNode::down);
        NodeRegistry registry2 = new NodeRegistry(NODES, 3, 3, dataDir);
        Coordinator coordinator2 = new Coordinator(registry2, 2, 2);

        ReadResult g1 = coordinator2.get("cv:6a");
        ReadResult g2 = coordinator2.get("cv:6b");
        ReadResult g3 = coordinator2.get("cv:6c");
        log.info("[业务:按需收敛][用例:全库收敛] 重启后 read: cv:6a={} cv:6b={} cv:6c={} 期望=OK/NOT_FOUND/OK",
                g1.status(), g2.status(), g3.status());
        assertEquals(Status.OK, g1.status());
        assertEquals("committed", g1.values().get(0).value());
        assertEquals(Status.NOT_FOUND, g2.status(), "被清除的残留重启后不得复活");
        assertEquals(Status.OK, g3.status());

        var report2 = coordinator2.inspectAll();
        log.info("[业务:按需收敛][用例:全库收敛] 重启后巡检 summary={}", report2.summary());
        for (var kr : report2.keys()) {
            assertEquals(Diagnosis.CONSISTENT, kr.diagnosis(), "重启后 " + kr.key() + " 应保持一致");
        }
    }
}
