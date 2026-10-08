package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.Diagnosis;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.InspectReport;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.KeyReport;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.Results.Status;
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
 * 业务场景：只读一致性巡检——对比各副本版本，
 * 区分"正常提交后暂时落后"与"未提交干净的残留"，且巡检本身不改动任何数据。
 * 集群：5 节点、3 分片、每分片 3 副本，W=2、R=2。
 */
class InspectionTest {
    private static final Logger log = LoggerFactory.getLogger(InspectionTest.class);
    private static final List<String> NODES = List.of("n1", "n2", "n3", "n4", "n5");

    @TempDir
    Path dataDir;

    NodeRegistry registry;
    Coordinator coordinator;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry(NODES, 3, 3, dataDir);
        coordinator = new Coordinator(registry, 2, 2);
        log.info("[业务:一致性巡检] 初始化集群 W=2 R=2");
    }

    /** 在指定副本上手工种一个未提交残留（模拟 prepare 后没等到决定的遗留）。 */
    private void plantResidue(String nodeId, String opId, String key, String value) {
        VectorClock clock = new VectorClock();
        clock.increment("w:" + opId);
        ReplicaNode.WriteAck ack = registry.node(nodeId).localPut(opId, key, value, null, clock, false);
        assertTrue(ack.accepted(), "种残留应被 prepare 接受");
    }

    @Test
    void consistentKeyReportsConsistent() {
        coordinator.put("op-i1", "insp:1", "v", null);

        KeyReport r = coordinator.inspect("insp:1").keys().get(0);
        log.info("[业务:一致性巡检][用例:一致键] diagnosis={} message={} 期望=CONSISTENT",
                r.diagnosis(), r.message());
        assertEquals(Diagnosis.CONSISTENT, r.diagnosis());
        assertTrue(r.laggingNodes().isEmpty());
        assertTrue(r.residueNodes().isEmpty());
    }

    @Test
    void laggingReplicaIsDetectedAndDistinguishedFromResidue() {
        String key = "insp:2";
        List<String> replicas = registry.router().replicasOf(key);
        ReplicaNode staleNode = registry.node(replicas.get(0));
        staleNode.down();
        coordinator.put("op-i2", key, "committed", null); // W=2 仍满足，写成功
        staleNode.up(); // 重新上线：它错过了这次写

        KeyReport r = coordinator.inspect(key).keys().get(0);
        log.info("[业务:一致性巡检][用例:落后副本识别] diagnosis={} lagging={} residue={} 期望=LAGGING/[{}]/[]",
                r.diagnosis(), r.laggingNodes(), r.residueNodes(), replicas.get(0));
        assertEquals(Diagnosis.LAGGING, r.diagnosis());
        assertEquals(List.of(replicas.get(0)), r.laggingNodes(), "应指出具体落后的副本");
        assertTrue(r.residueNodes().isEmpty(), "落后不是未提交残留，两者必须区分");
    }

    @Test
    void uncommittedResidueIsDetectedAndDistinguishedFromLag() {
        String key = "insp:3";
        List<String> replicas = registry.router().replicasOf(key);
        plantResidue(replicas.get(1), "op-ghost", key, "never-committed");

        KeyReport r = coordinator.inspect(key).keys().get(0);
        log.info("[业务:一致性巡检][用例:未提交残留识别] diagnosis={} residue={} 期望=UNCOMMITTED_RESIDUE/[{}]",
                r.diagnosis(), r.residueNodes(), replicas.get(1));
        assertEquals(Diagnosis.UNCOMMITTED_RESIDUE, r.diagnosis());
        assertEquals(List.of(replicas.get(1)), r.residueNodes());
        assertTrue(r.laggingNodes().isEmpty(), "残留不是落后，两者必须区分");

        // 报告里能看到残留的明细（opId 与值）
        var snap = r.replicas().stream().filter(s -> s.nodeId().equals(replicas.get(1))).findFirst().orElseThrow();
        assertEquals(1, snap.pending().size());
        assertEquals("op-ghost", snap.pending().get(0).opId());
        assertEquals("never-committed", snap.pending().get(0).value().value());
    }

    @Test
    void inspectionIsReadOnlyAndDoesNotRepairAnything() {
        String key = "insp:4";
        List<String> replicas = registry.router().replicasOf(key);
        ReplicaNode staleNode = registry.node(replicas.get(0));
        staleNode.down();
        coordinator.put("op-i4", key, "committed", null);
        staleNode.up();
        plantResidue(replicas.get(1), "op-ghost-4", key, "residue");

        ReplicaNode.Snapshot before0 = registry.node(replicas.get(0)).localInspect(key);
        ReplicaNode.Snapshot before1 = registry.node(replicas.get(1)).localInspect(key);

        KeyReport r1 = coordinator.inspect(key).keys().get(0);
        KeyReport r2 = coordinator.inspect(key).keys().get(0);
        log.info("[业务:一致性巡检][用例:巡检只读] 第一次={} 第二次={} 期望=MIXED/MIXED（重复巡检结果一致）",
                r1.diagnosis(), r2.diagnosis());
        assertEquals(Diagnosis.MIXED, r1.diagnosis());
        assertEquals(r1.diagnosis(), r2.diagnosis(), "重复巡检结果应一致");

        ReplicaNode.Snapshot after0 = registry.node(replicas.get(0)).localInspect(key);
        ReplicaNode.Snapshot after1 = registry.node(replicas.get(1)).localInspect(key);
        log.info("[业务:一致性巡检][用例:巡检只读] 落后副本committed={}->{} 残留副本pending={}->{} 期望=不变",
                before0.committed().size(), after0.committed().size(),
                before1.pending().size(), after1.pending().size());
        assertEquals(before0.committed(), after0.committed(), "巡检不得修复落后副本");
        assertEquals(before1.pending().size(), after1.pending().size(), "巡检不得清除残留");
    }

    @Test
    void inspectAllCoversEveryKey() {
        coordinator.put("op-i5a", "insp:5a", "ok", null);

        List<String> replicasB = registry.router().replicasOf("insp:5b");
        ReplicaNode stale = registry.node(replicasB.get(0));
        stale.down();
        coordinator.put("op-i5b", "insp:5b", "lagging", null);
        stale.up();

        List<String> replicasC = registry.router().replicasOf("insp:5c");
        plantResidue(replicasC.get(2), "op-ghost-5", "insp:5c", "residue");

        InspectReport report = coordinator.inspectAll();
        log.info("[业务:一致性巡检][用例:全库巡检] summary={}", report.summary());
        assertEquals(3, report.keys().size(), "全库巡检应覆盖所有出现过的键");

        for (KeyReport kr : report.keys()) {
            log.info("[业务:一致性巡检][用例:全库巡检] key={} diagnosis={}", kr.key(), kr.diagnosis());
        }
        assertEquals(Diagnosis.CONSISTENT, findKey(report, "insp:5a").diagnosis());
        assertEquals(Diagnosis.LAGGING, findKey(report, "insp:5b").diagnosis());
        assertEquals(Diagnosis.UNCOMMITTED_RESIDUE, findKey(report, "insp:5c").diagnosis());
    }

    private static KeyReport findKey(InspectReport report, String key) {
        return report.keys().stream().filter(k -> k.key().equals(key)).findFirst().orElseThrow();
    }
}
