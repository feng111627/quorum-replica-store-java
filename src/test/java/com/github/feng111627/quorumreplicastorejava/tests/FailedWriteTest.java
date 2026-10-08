package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.Status;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
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
 * 业务场景：写失败（未凑够法定人数）必须干净收场——
 * 当作从未发生：任何副本都读不到它，重启后不复活，不与后续写纠缠。
 * 集群：5 节点、3 分片、每分片 3 副本，W=2、R=2。
 */
class FailedWriteTest {
    private static final Logger log = LoggerFactory.getLogger(FailedWriteTest.class);
    private static final List<String> NODES = List.of("n1", "n2", "n3", "n4", "n5");

    @TempDir
    Path dataDir;

    NodeRegistry registry;
    Coordinator coordinator;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry(NODES, 3, 3, dataDir);
        coordinator = new Coordinator(registry, 2, 2);
        log.info("[业务:写失败清理] 初始化集群 W=2 R=2");
    }

    /** 关掉该键 3 个副本中的 2 个，使 W=2 达不到；返回副本 id 列表。 */
    private List<String> downTwoReplicas(String key) {
        List<String> replicas = registry.router().replicasOf(key);
        registry.node(replicas.get(0)).down();
        registry.node(replicas.get(1)).down();
        return replicas;
    }

    private void assertNoTraceAnywhere(String key, List<String> replicas, String caseName) {
        for (String id : replicas) {
            ReplicaNode.Snapshot snap = registry.node(id).localInspect(key);
            if (snap == null) continue; // 仍下线的副本跳过
            log.info("[业务:写失败清理][用例:{}] 副本 {} committed={} pending={} 期望=0/0",
                    caseName, id, snap.committed().size(), snap.pending().size());
            assertEquals(0, snap.committed().size(), "副本 " + id + " 不得持有失败写的已提交版本");
            assertEquals(0, snap.pending().size(), "副本 " + id + " 不得残留失败写的未提交版本");
        }
    }

    @Test
    void failedWriteOnNewKeyReadsAsNotFound() {
        String key = "fw:1";
        List<String> replicas = downTwoReplicas(key);
        log.info("[业务:写失败清理][用例:失败写如新键] 关闭副本 {} {}", replicas.get(0), replicas.get(1));

        WriteResult w = coordinator.put("op-fw1", key, "ghost", null);
        log.info("[业务:写失败清理][用例:失败写如新键] write={} 期望=WRITE_QUORUM_NOT_MET", w.status());
        assertEquals(Status.WRITE_QUORUM_NOT_MET, w.status());

        // 恢复全部副本后读：失败写当作从未发生
        registry.node(replicas.get(0)).up();
        registry.node(replicas.get(1)).up();
        ReadResult g = coordinator.get(key);
        log.info("[业务:写失败清理][用例:失败写如新键] read={} 期望=NOT_FOUND", g.status());
        assertEquals(Status.NOT_FOUND, g.status());

        assertNoTraceAnywhere(key, replicas, "失败写如新键");
    }

    @Test
    void failedWriteDoesNotDisturbPreviousCommittedValue() {
        String key = "fw:2";
        WriteResult w1 = coordinator.put("op-fw2-a", key, "committed-v1", null);
        assertEquals(Status.OK, w1.status());

        List<String> replicas = downTwoReplicas(key);
        WriteResult w2 = coordinator.put("op-fw2-b", key, "failed-v2", w1.context());
        log.info("[业务:写失败清理][用例:失败写不扰动已提交值] write2={} 期望=WRITE_QUORUM_NOT_MET", w2.status());
        assertEquals(Status.WRITE_QUORUM_NOT_MET, w2.status());

        registry.node(replicas.get(0)).up();
        registry.node(replicas.get(1)).up();
        ReadResult g = coordinator.get(key);
        log.info("[业务:写失败清理][用例:失败写不扰动已提交值] read={} value={} 期望=OK/committed-v1",
                g.status(), g.values().isEmpty() ? null : g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("committed-v1", g.values().get(0).value());
    }

    @Test
    void failedWriteDoesNotResurrectAfterFullClusterRestart() {
        String key = "fw:3";
        List<String> replicas = downTwoReplicas(key);
        WriteResult w = coordinator.put("op-fw3", key, "ghost", null);
        assertEquals(Status.WRITE_QUORUM_NOT_MET, w.status());
        log.info("[业务:写失败清理][用例:重启后不复活] 写失败后全集群重启");

        // 全部节点宕机，用同一数据目录重建集群（模拟整机重启）
        registry.all().values().forEach(ReplicaNode::down);
        NodeRegistry registry2 = new NodeRegistry(NODES, 3, 3, dataDir);
        Coordinator coordinator2 = new Coordinator(registry2, 2, 2);

        ReadResult g = coordinator2.get(key);
        log.info("[业务:写失败清理][用例:重启后不复活] read={} 期望=NOT_FOUND", g.status());
        assertEquals(Status.NOT_FOUND, g.status());
        assertNoTraceAnywhere(key, registry2.router().replicasOf(key).stream()
                .map(id -> id).toList(), "重启后不复活");
        // 用新注册表再确认各副本无残留
        for (String id : registry2.router().replicasOf(key)) {
            ReplicaNode.Snapshot snap = registry2.node(id).localInspect(key);
            assertEquals(0, snap.committed().size());
            assertEquals(0, snap.pending().size());
        }
    }

    @Test
    void failedWriteDoesNotConflictWithLaterSuccessfulWrite() {
        String key = "fw:4";
        List<String> replicas = downTwoReplicas(key);
        WriteResult w1 = coordinator.put("op-fw4-a", key, "failed-x", null);
        assertEquals(Status.WRITE_QUORUM_NOT_MET, w1.status());

        registry.node(replicas.get(0)).up();
        registry.node(replicas.get(1)).up();
        WriteResult w2 = coordinator.put("op-fw4-b", key, "success-y", null);
        log.info("[业务:写失败清理][用例:失败写不与后续写冲突] write2={} 期望=OK", w2.status());
        assertEquals(Status.OK, w2.status());

        ReadResult g = coordinator.get(key);
        log.info("[业务:写失败清理][用例:失败写不与后续写冲突] read={} value={} 期望=OK/success-y（不得出现CONFLICT）",
                g.status(), g.values().isEmpty() ? null : g.values().get(0).value());
        assertEquals(Status.OK, g.status(), "失败写不得作为兄弟版本与后续成功写纠缠");
        assertEquals("success-y", g.values().get(0).value());
    }

    @Test
    void retryWithSameOpIdAfterFailureStartsClean() {
        String key = "fw:5";
        List<String> replicas = downTwoReplicas(key);
        WriteResult w1 = coordinator.put("op-fw5", key, "attempt-1", null);
        assertEquals(Status.WRITE_QUORUM_NOT_MET, w1.status());

        // 客户端用同一 opId 重试：第一次已彻底撤销，重试是干净的新写
        registry.node(replicas.get(0)).up();
        registry.node(replicas.get(1)).up();
        WriteResult w2 = coordinator.put("op-fw5", key, "attempt-1", null);
        log.info("[业务:写失败清理][用例:同opId重试] retry={} 期望=OK", w2.status());
        assertEquals(Status.OK, w2.status());

        ReadResult g = coordinator.get(key);
        log.info("[业务:写失败清理][用例:同opId重试] read={} value={} 版本数={} 期望=OK/attempt-1/1",
                g.status(), g.values().isEmpty() ? null : g.values().get(0).value(), g.values().size());
        assertEquals(Status.OK, g.status());
        assertEquals("attempt-1", g.values().get(0).value());
        assertEquals(1, g.values().size(), "重试不得产生重复版本");
    }

    @Test
    void failedDeleteDoesNotRemoveCommittedValue() {
        String key = "fw:6";
        WriteResult w1 = coordinator.put("op-fw6-a", key, "keep-me", null);
        assertEquals(Status.OK, w1.status());

        List<String> replicas = downTwoReplicas(key);
        WriteResult d = coordinator.delete("op-fw6-b", key, w1.context());
        log.info("[业务:写失败清理][用例:失败删除不生效] delete={} 期望=WRITE_QUORUM_NOT_MET", d.status());
        assertEquals(Status.WRITE_QUORUM_NOT_MET, d.status());

        registry.node(replicas.get(0)).up();
        registry.node(replicas.get(1)).up();
        ReadResult g = coordinator.get(key);
        log.info("[业务:写失败清理][用例:失败删除不生效] read={} value={} 期望=OK/keep-me",
                g.status(), g.values().isEmpty() ? null : g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("keep-me", g.values().get(0).value());
    }
}
