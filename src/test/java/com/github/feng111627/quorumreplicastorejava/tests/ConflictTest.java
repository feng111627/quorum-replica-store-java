package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 业务场景：副本间写入冲突检测与处理。
 * 集群：5 节点、3 分片、每分片 3 副本，W=2、R=2。
 */
class ConflictTest {
    private static final Logger log = LoggerFactory.getLogger(ConflictTest.class);
    private static final List<String> NODES = List.of("n1", "n2", "n3", "n4", "n5");

    @TempDir
    Path dataDir;

    NodeRegistry registry;
    Coordinator coordinator;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry(NODES, 3, 3, dataDir);
        coordinator = new Coordinator(registry, 2, 2);
        log.info("[业务:并发写冲突] 初始化集群 W=2 R=2");
    }

    @Test
    void concurrentWritesAreDetectedAsConflict() {
        // 两个客户端基于同一上下文（都读过初始版本）并发写不同的值
        WriteResult w1 = coordinator.put("op-a", "k1", "v1", null);
        assertEquals(Status.OK, w1.status());
        VectorClock base = w1.context(); // 两个客户端都拿到了这个上下文

        WriteResult w2 = coordinator.put("op-b", "k1", "v2-from-client-B", base);
        WriteResult w3 = coordinator.put("op-c", "k1", "v3-from-client-C", base);
        log.info("[业务:并发写冲突][用例:并发写检测] writeB={} writeC={}", w2.status(), w3.status());
        assertEquals(Status.OK, w2.status());
        assertEquals(Status.OK, w3.status());

        ReadResult g = coordinator.get("k1");
        log.info("[业务:并发写冲突][用例:并发写检测] read={} siblings={} 期望=CONFLICT",
                g.status(), g.values().size());
        assertEquals(Status.CONFLICT, g.status());
        assertEquals(2, g.values().size(), "两个并发写都应作为兄弟版本暴露");
    }

    @Test
    void conflictResolvedByClientWithMergedContext() {
        coordinator.put("op-a", "k2", "base", null);
        ReadResult r0 = coordinator.get("k2");
        VectorClock base = r0.context();

        coordinator.put("op-b", "k2", "B", base);
        coordinator.put("op-c", "k2", "C", base);
        ReadResult conflict = coordinator.get("k2");
        assertEquals(Status.CONFLICT, conflict.status());

        // 客户端用冲突读返回的合并上下文做解决写
        WriteResult resolved = coordinator.put("op-d", "k2", "resolved", conflict.context());
        log.info("[业务:并发写冲突][用例:冲突解决] resolve={} 期望=OK", resolved.status());
        assertEquals(Status.OK, resolved.status());

        ReadResult after = coordinator.get("k2");
        log.info("[业务:并发写冲突][用例:冲突解决] read={} value={} 期望=OK/resolved",
                after.status(), after.values().isEmpty() ? null : after.values().get(0).value());
        assertEquals(Status.OK, after.status());
        assertEquals("resolved", after.values().get(0).value());
    }

    @Test
    void staleWriteCannotOverwriteCommittedData() {
        // 提交 v1，再基于 v1 提交 v2；之后重放一个过期写（时钟被 v2 支配）必须被拒绝
        coordinator.put("op-1", "k3", "v1", null);
        ReadResult r = coordinator.get("k3");
        coordinator.put("op-2", "k3", "v2", r.context());

        // 构造一个被已提交版本支配的过期写，直接打到副本上验证拒绝逻辑
        VectorClock staleClock = new VectorClock(Map.of("w:op-1", 1L)); // 只含 op-1，落后于已提交版本
        List<String> replicas = registry.router().replicasOf("k3");
        ReplicaNode node = registry.node(replicas.get(0));
        ReplicaNode.WriteAck ack = node.localPrepare("op-stale", "k3", "old-value", staleClock, false);
        log.info("[业务:并发写冲突][用例:过期写拒绝] accepted={} stale={} 期望=false/true",
                ack.accepted(), ack.stale());
        assertFalse(ack.accepted());
        assertTrue(ack.stale());

        ReadResult after = coordinator.get("k3");
        log.info("[业务:并发写冲突][用例:过期写拒绝] read={} value={} 期望=OK/v2（已提交数据不被覆盖）",
                after.status(), after.values().get(0).value());
        assertEquals(Status.OK, after.status());
        assertEquals("v2", after.values().get(0).value());
    }

    @Test
    void sequentialWritesWithReadContextDoNotConflict() {
        coordinator.put("op-1", "k4", "v1", null);
        ReadResult r1 = coordinator.get("k4");
        WriteResult w2 = coordinator.put("op-2", "k4", "v2", r1.context());
        ReadResult r2 = coordinator.get("k4");
        log.info("[业务:并发写冲突][用例:顺序写无冲突] write2={} read={} value={} 期望=OK/OK/v2",
                w2.status(), r2.status(), r2.values().get(0).value());
        assertEquals(Status.OK, w2.status());
        assertEquals(Status.OK, r2.status());
        assertEquals("v2", r2.values().get(0).value());
        assertEquals(1, r2.values().size(), "顺序写不应产生兄弟版本");
    }
}
