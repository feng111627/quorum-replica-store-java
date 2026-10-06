package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.Status;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 业务场景：读写法定人数（quorum）判定。
 * 集群：5 节点、3 分片、每分片 3 副本，W=2、R=2。
 */
class QuorumTest {
    private static final Logger log = LoggerFactory.getLogger(QuorumTest.class);
    private static final List<String> NODES = List.of("n1", "n2", "n3", "n4", "n5");

    @TempDir
    Path dataDir;

    NodeRegistry registry;
    Coordinator coordinator;

    @BeforeEach
    void setUp() {
        registry = new NodeRegistry(NODES, 3, 3, dataDir);
        coordinator = new Coordinator(registry, 2, 2);
        log.info("[业务:读写法定人数] 初始化集群 nodes={} shardCount=3 replicas=3 W=2 R=2", NODES);
    }

    @Test
    void writeSucceedsWhenQuorumMet() {
        WriteResult r = coordinator.put("op-1", "user:1", "alice", null);
        log.info("[业务:读写法定人数][用例:写达到法定人数] status={} 期望=OK", r.status());
        assertEquals(Status.OK, r.status());

        ReadResult g = coordinator.get("user:1");
        log.info("[业务:读写法定人数][用例:读已提交数据] status={} value={}", g.status(),
                g.values().isEmpty() ? null : g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("alice", g.values().get(0).value());
    }

    @Test
    void writeFailsWhenQuorumNotMet() {
        // 关掉该键 3 个副本中的 2 个，只剩 1 个可写，W=2 达不到
        List<String> replicas = registry.router().replicasOf("user:2");
        registry.node(replicas.get(0)).down();
        registry.node(replicas.get(1)).down();
        log.info("[业务:读写法定人数][用例:写法定人数不足] 关闭副本 {} 和 {}", replicas.get(0), replicas.get(1));

        WriteResult r = coordinator.put("op-2", "user:2", "bob", null);
        log.info("[业务:读写法定人数][用例:写法定人数不足] status={} message={} 期望=WRITE_QUORUM_NOT_MET",
                r.status(), r.message());
        assertEquals(Status.WRITE_QUORUM_NOT_MET, r.status());
        assertNotEquals(Status.OK, r.status(), "确认数不足时不允许悄悄成功");
    }

    @Test
    void readFailsWhenQuorumNotMet() {
        WriteResult w = coordinator.put("op-3", "user:3", "carol", null);
        assertEquals(Status.OK, w.status());

        List<String> replicas = registry.router().replicasOf("user:3");
        registry.node(replicas.get(0)).down();
        registry.node(replicas.get(1)).down();
        log.info("[业务:读写法定人数][用例:读法定人数不足] 关闭副本 {} 和 {}", replicas.get(0), replicas.get(1));

        ReadResult g = coordinator.get("user:3");
        log.info("[业务:读写法定人数][用例:读法定人数不足] status={} message={} 期望=READ_QUORUM_NOT_MET（不得返回旧值）",
                g.status(), g.message());
        assertEquals(Status.READ_QUORUM_NOT_MET, g.status());
        assertTrue(g.values().isEmpty(), "法定人数不足时不得返回部分结果");
    }

    @Test
    void readAndWriteSucceedWithExactlyQuorumNodes() {
        List<String> replicas = registry.router().replicasOf("user:4");
        registry.node(replicas.get(2)).down(); // 只关 1 个，还剩 2 个，刚好满足 W=2/R=2
        log.info("[业务:读写法定人数][用例:恰好法定人数] 关闭副本 {}", replicas.get(2));

        WriteResult w = coordinator.put("op-4", "user:4", "dave", null);
        ReadResult g = coordinator.get("user:4");
        log.info("[业务:读写法定人数][用例:恰好法定人数] write={} read={} 期望=OK/OK", w.status(), g.status());
        assertEquals(Status.OK, w.status());
        assertEquals(Status.OK, g.status());
        assertEquals("dave", g.values().get(0).value());
    }

    @Test
    void readOfMissingKeyReturnsNotFound() {
        ReadResult g = coordinator.get("no-such-key");
        log.info("[业务:读写法定人数][用例:读不存在的键] status={} 期望=NOT_FOUND", g.status());
        assertEquals(Status.NOT_FOUND, g.status());
    }
}
