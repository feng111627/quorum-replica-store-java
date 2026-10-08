package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.EntryState;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.ClusterReport;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.DiffKind;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.KeyReport;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.ReplicaDiff;
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
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 业务场景：只读一致性巡检。
 * 验证点：一致键的判定、正常「已提交后落后」（MISSING/LAGGING）与
 * 「未提交干净残留」（UNCOMMITTED_RESIDUE）的区分、下线节点呈现、
 * 整库巡检汇总、以及巡检本身绝不修改任何数据。
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
        coordinator = new Coordinator(registry, 2, 2, dataDir);
        log.info("[业务:一致性巡检] 初始化集群 W=2 R=2");
    }

    private List<String> replicas(String key) {
        return registry.router().replicasOf(key);
    }

    private Map<String, DiffKind> kinds(KeyReport report) {
        return report.replicas().stream()
                .collect(Collectors.toMap(ReplicaDiff::nodeId, ReplicaDiff::kind));
    }

    @Test
    void inspectConsistentKeyAfterSuccessfulWrite() {
        String key = "insp-ok";
        assertEquals(Status.OK, coordinator.put("op-1", key, "v1", null).status());
        coordinator.get(key); // 读修复把第三个副本也补齐

        KeyReport report = coordinator.inspectKey(key);
        log.info("[业务:一致性巡检][用例:一致键] consistent={} kinds={} summary={}",
                report.consistent(), kinds(report), report.summary());
        assertTrue(report.consistent());
        assertTrue(report.residueVersions().isEmpty());
        assertEquals(1, report.committedClocks().size());
        assertTrue(kinds(report).values().stream().allMatch(k -> k == DiffKind.CONSISTENT));
    }

    @Test
    void inspectDistinguishesCommittedLaggingReplica() {
        String key = "insp-lag";
        List<String> r = replicas(key);
        // 关掉一个副本提交成功（W=2），不读修复：它就是「提交完成后暂时落后」而非残留
        ReplicaNode lagging = registry.node(r.get(0));
        lagging.down();
        WriteResult w = coordinator.put("op-1", key, "committed", null);
        assertEquals(Status.OK, w.status());
        lagging.up();

        KeyReport report = coordinator.inspectKey(key);
        Map<String, DiffKind> ks = kinds(report);
        log.info("[业务:一致性巡检][用例:落后副本定性] kinds={} {}", ks,
                report.replicas().stream().filter(d -> d.nodeId().equals(r.get(0))).findFirst().orElseThrow().detail());
        assertFalse(report.consistent());
        assertTrue(report.residueVersions().isEmpty(), "落后不是残留，residue 必须为空");
        assertNotEquals(DiffKind.CONSISTENT, ks.get(r.get(0)));
        assertTrue(ks.get(r.get(0)) == DiffKind.MISSING_COMMITTED || ks.get(r.get(0)) == DiffKind.COMMITTED_LAGGING,
                "落后副本应被标为 MISSING_COMMITTED/COMMITTED_LAGGING");
    }

    @Test
    void inspectDistinguishesUncommittedResidueFromLagging() {
        String key = "insp-residue";
        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down(); // 只剩 r[2] 收到预备值
        assertEquals(Status.WRITE_QUORUM_NOT_MET,
                coordinator.put("fail-1", key, "ghost", null).status());
        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();

        KeyReport report = coordinator.inspectKey(key);
        Map<String, DiffKind> ks = kinds(report);
        log.info("[业务:一致性巡检][用例:残留与落后区分] kinds={} residue={}",
                ks, report.residueVersions().stream().map(v -> v.nodeId() + ":" + v.state()).toList());
        assertEquals(DiffKind.UNCOMMITTED_RESIDUE, ks.get(r.get(2)),
                "收到未确认写的副本必须标为未提交残留");
        assertNotEquals(DiffKind.UNCOMMITTED_RESIDUE, ks.get(r.get(0)),
                "什么都没收到的副本不是残留");
        assertEquals(1, report.residueVersions().size());
        assertEquals(EntryState.ABORTED, report.residueVersions().get(0).state());
    }

    @Test
    void inspectReportsDownReplica() {
        String key = "insp-down";
        assertEquals(Status.OK, coordinator.put("op-1", key, "v", null).status());
        List<String> r = replicas(key);
        registry.node(r.get(1)).down();

        KeyReport report = coordinator.inspectKey(key);
        log.info("[业务:一致性巡检][用例:下线副本呈现] kinds={}", kinds(report));
        assertEquals(DiffKind.NODE_DOWN, kinds(report).get(r.get(1)));
        assertFalse(report.consistent());
    }

    @Test
    void inspectAllSummarizesCluster() {
        coordinator.put("op-1", "insp-all-1", "v", null);
        String bad = "insp-all-2";
        List<String> r = replicas(bad);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        coordinator.put("fail-1", bad, "ghost", null);
        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();

        ClusterReport report = coordinator.inspectAll();
        log.info("[业务:一致性巡检][用例:整库巡检] keyCount={} inconsistent={} summary={}",
                report.keyCount(), report.inconsistentKeys(), report.summary());
        assertTrue(report.keyCount() >= 2);
        assertTrue(report.inconsistentKeys().contains(bad));
    }

    @Test
    void inspectionIsReadOnly() {
        String key = "insp-readonly";
        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        coordinator.put("fail-1", key, "ghost", null);
        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();

        // 巡检前抓取各副本版本快照
        List<List<StoredEntry>> before = r.stream()
                .map(id -> List.copyOf(registry.node(id).localEntries(key))).toList();
        coordinator.inspectKey(key);
        coordinator.inspectAll();
        List<List<StoredEntry>> after = r.stream()
                .map(id -> List.copyOf(registry.node(id).localEntries(key))).toList();

        log.info("[业务:一致性巡检][用例:巡检只读] 前后快照相等={}", before.equals(after));
        assertEquals(before, after, "巡检不得改动任何副本数据");
    }
}
