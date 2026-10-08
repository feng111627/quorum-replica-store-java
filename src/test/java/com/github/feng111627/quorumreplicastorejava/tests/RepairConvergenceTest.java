package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.KeyReport;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.KeyReport;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.RepairResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.Status;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
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
 * 业务场景：按需收敛（repair）。
 * 验证点：补齐正常落后副本、清除未提交残留且绝不提升它、重复/中途再跑结果幂等、
 * 收敛不破坏并发已提交兄弟、下线副本上线后再收敛即一致、整库收敛、整机重启后仍正确。
 */
class RepairConvergenceTest {
    private static final Logger log = LoggerFactory.getLogger(RepairConvergenceTest.class);
    private static final List<String> NODES = List.of("n1", "n2", "n3", "n4", "n5");

    @TempDir
    Path dataDir;

    NodeRegistry registry;
    Coordinator coordinator;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry(NODES, 3, 3, dataDir);
        coordinator = new Coordinator(registry, 2, 2, dataDir);
        log.info("[业务:按需收敛] 初始化集群 W=2 R=2");
    }

    private List<String> replicas(String key) {
        return registry.router().replicasOf(key);
    }

    @Test
    void repairHealsCommittedLaggingReplica() {
        String key = "rep-lag";
        List<String> r = replicas(key);
        ReplicaNode lagging = registry.node(r.get(0));
        lagging.down();
        assertEquals(Status.OK, coordinator.put("op-1", key, "committed", null).status());
        lagging.up(); // 落后副本重新加入，不触发读修复，直接收敛

        RepairResult result = coordinator.repair(key);
        log.info("[业务:按需收敛][用例:补齐落后副本] {}", result.summary());
        assertTrue(result.changed());

        ReadResult g = coordinator.get(key);
        assertEquals(Status.OK, g.status());
        assertEquals("committed", g.values().get(0).value());
        assertTrue(coordinator.inspectKey(key).consistent(), "收敛后巡检应一致");
        assertEquals(1, lagging.localCommittedGet(key).size());
    }

    @Test
    void repairPurgesUncommittedResidueWithoutPromotingIt() {
        String key = "rep-residue";
        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        assertEquals(Status.WRITE_QUORUM_NOT_MET,
                coordinator.put("fail-1", key, "ghost", null).status());
        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();
        assertFalse(coordinator.inspectKey(key).consistent());

        RepairResult result = coordinator.repair(key);
        log.info("[业务:按需收敛][用例:清除未提交残留] {} decisions={}", result.summary(), result.decisions());
        assertTrue(result.residuePurged() >= 1);

        ReadResult g = coordinator.get(key);
        log.info("[业务:按需收敛][用例:清除未提交残留] 收敛后 read={} 期望=NOT_FOUND（ghost 绝不能被提升）", g.status());
        assertEquals(Status.NOT_FOUND, g.status());
        KeyReport report = coordinator.inspectKey(key);
        assertTrue(report.consistent());
        assertTrue(report.residueVersions().isEmpty());
    }

    @Test
    void repairRestoresOldValueWhenFailedWriteLeftResidue() {
        String key = "rep-old";
        assertEquals(Status.OK, coordinator.put("ok-1", key, "before", null).status());
        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        assertEquals(Status.WRITE_QUORUM_NOT_MET,
                coordinator.put("fail-2", key, "ghost", null).status());
        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();

        coordinator.repair(key);
        ReadResult g = coordinator.get(key);
        log.info("[业务:按需收敛][用例:残留清除后仍是旧值] read={} value={} 期望=OK/before",
                g.status(), g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("before", g.values().get(0).value());
    }

    @Test
    void repairIsIdempotentAcrossRepeatedAndConcurrentRuns() {
        String key = "rep-idem";
        List<String> r = replicas(key);
        ReplicaNode lagging = registry.node(r.get(0));
        lagging.down();
        coordinator.put("op-1", key, "v1", null);
        // 再制造一个未提交残留
        registry.node(r.get(1)).down();
        coordinator.put("fail-x", key + "-sibling", "ghost", null);
        lagging.up();
        registry.node(r.get(1)).up();

        RepairResult first = coordinator.repair(key);
        // 抓取收敛后各副本版本快照
        List<List<VersionedValue>> snap1 = r.stream()
                .map(id -> registry.node(id).localCommittedGet(key)).map(List::copyOf).toList();

        RepairResult second = coordinator.repair(key); // 重复触发
        RepairResult third = coordinator.repair(key);  // 再跑一遍（模拟中途重跑）
        List<List<VersionedValue>> snap2 = r.stream()
                .map(id -> registry.node(id).localCommittedGet(key)).map(List::copyOf).toList();

        log.info("[业务:按需收敛][用例:重复收敛幂等] first.changed={} second.changed={} third.changed={} 版本快照相等={}",
                first.changed(), second.changed(), third.changed(), snap1.equals(snap2));
        assertTrue(first.changed());
        assertFalse(second.changed(), "第二次收敛不应再有改动");
        assertFalse(third.changed());
        assertEquals(snap1, snap2, "重复收敛不得搞乱版本");
        assertTrue(coordinator.inspectKey(key).consistent());
    }

    @Test
    void repairPreservesConcurrentCommittedSiblings() {
        String key = "rep-conflict";
        WriteResult w1 = coordinator.put("op-a", key, "A", null);
        VectorClock base = w1.context();
        coordinator.put("op-b", key, "B", base);
        coordinator.put("op-c", key, "C", base); // 两个并发已提交兄弟

        coordinator.repair(key);
        ReadResult g = coordinator.get(key);
        log.info("[业务:按需收敛][用例:不破坏并发已提交兄弟] read={} siblings={} 期望=CONFLICT/2",
                g.status(), g.values().size());
        assertEquals(Status.CONFLICT, g.status());
        assertEquals(2, g.values().size());
        // 副本间一致（都持相同兄弟集），但业务冲突仍需客户端解决
        assertTrue(coordinator.inspectKey(key).consistent());
    }

    @Test
    void repairWaitsForDownReplicaThenHealsOnNextRun() {
        String key = "rep-down";
        List<String> r = replicas(key);
        ReplicaNode down = registry.node(r.get(2));
        down.down();
        coordinator.put("op-1", key, "v", null); // W=2 成功，down 副本缺数据

        RepairResult r1 = coordinator.repair(key); // 下线副本不被触碰
        log.info("[业务:按需收敛][用例:下线副本再收敛] 首次 reached={}/{} changed={}",
                r1.replicasReached(), r1.replicasReached() + 1, r1.changed());
        assertEquals(2, r1.replicasReached());

        down.up();
        RepairResult r2 = coordinator.repair(key);
        log.info("[业务:按需收敛][用例:下线副本再收敛] 上线后再收敛 changed={}", r2.changed());
        assertTrue(r2.changed());
        assertTrue(coordinator.inspectKey(key).consistent());
    }

    @Test
    void repairAllHealsEveryKeyAndStaysStable() {
        coordinator.put("op-1", "rep-all-1", "v1", null);
        coordinator.put("op-2", "rep-all-2", "v2", null);
        String bad = "rep-all-3";
        List<String> r = replicas(bad);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        coordinator.put("fail-1", bad, "ghost", null);
        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();

        List<RepairResult> first = coordinator.repairAll();
        List<RepairResult> second = coordinator.repairAll();
        log.info("[业务:按需收敛][用例:整库收敛] 首次键数={} 仍有改动的键={}",
                first.size(), second.stream().filter(RepairResult::changed).count());
        assertTrue(second.stream().noneMatch(RepairResult::changed), "整库二次收敛必须无改动（幂等）");
        assertTrue(coordinator.inspectAll().consistent());
    }

    @Test
    void purgedResidueStaysGoneAfterRestartAndRepairStaysStable() {
        String key = "rep-gc-restart";
        List<String> r = replicas(key);
        registry.node(r.get(0)).down();
        registry.node(r.get(1)).down();
        coordinator.put("fail-gc", key, "ghost", null);
        registry.node(r.get(0)).up();
        registry.node(r.get(1)).up();

        assertTrue(coordinator.repair(key).changed());
        assertTrue(coordinator.inspectKey(key).consistent());

        // 整机重启：GC 记录重放，残留不得重新出现
        registry.all().values().forEach(ReplicaNode::down);
        NodeRegistry registry2 = new NodeRegistry(NODES, 3, 3, dataDir);
        Coordinator coordinator2 = new Coordinator(registry2, 2, 2, dataDir);

        KeyReport report = coordinator2.inspectKey(key);
        log.info("[业务:按需收敛][用例:清理后重启仍稳定] consistent={} residue={}",
                report.consistent(), report.residueVersions().size());
        assertTrue(report.consistent(), "已物理回收的残留重启后不得重现");
        RepairResult again = coordinator2.repair(key);
        log.info("[业务:按需收敛][用例:清理后重启仍稳定] 重启后再收敛 changed={} 期望=false", again.changed());
        assertFalse(again.changed(), "重启后重复收敛必须仍然无改动");
    }

    @Test
    void repairAfterFullClusterRestartStillRefusesGhostAndHeals() {
        String key = "rep-restart";
        List<String> r = replicas(key);
        ReplicaNode lagging = registry.node(r.get(0));
        lagging.down();
        coordinator.put("ok-1", key, "committed", null);
        registry.node(r.get(1)).down();
        coordinator.put("fail-9", key, "ghost", null); // 仅 r[2] 收到预备
        lagging.up();
        registry.node(r.get(1)).up();

        // 整机重启
        registry.all().values().forEach(ReplicaNode::down);
        NodeRegistry registry2 = new NodeRegistry(NODES, 3, 3, dataDir);
        Coordinator coordinator2 = new Coordinator(registry2, 2, 2, dataDir);

        ReadResult before = coordinator2.get(key);
        log.info("[业务:按需收敛][用例:重启后收敛] 重启后直接读={} value={}（ghost 不可见）",
                before.status(), before.values().isEmpty() ? null : before.values().get(0).value());
        assertEquals("committed", before.values().get(0).value());

        RepairResult repaired = coordinator2.repair(key);
        ReadResult after = coordinator2.get(key);
        log.info("[业务:按需收敛][用例:重启后收敛] repair.changed={} read={} value={} 一致={}",
                repaired.changed(), after.status(),
                after.values().isEmpty() ? null : after.values().get(0).value(),
                coordinator2.inspectKey(key).consistent());
        assertEquals(Status.OK, after.status());
        assertEquals("committed", after.values().get(0).value());
        assertTrue(coordinator2.inspectKey(key).consistent());
    }
}
