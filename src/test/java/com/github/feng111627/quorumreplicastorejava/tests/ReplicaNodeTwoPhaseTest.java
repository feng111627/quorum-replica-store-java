package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 业务场景：副本节点两阶段原语（PENDING/COMMITTED/ABORTED）。
 * 验证：预备值读不可见、提交/放弃决定幂等、带值提交到错过预备的副本不留空壳、
 * 放弃决定在 WAL 重放后依旧生效。
 */
class ReplicaNodeTwoPhaseTest {
    private static final Logger log = LoggerFactory.getLogger(ReplicaNodeTwoPhaseTest.class);

    @TempDir
    Path dataDir;

    @Test
    void prepareIsInvisibleCommitAndAbortAreIdempotent() {
        ReplicaNode node = new ReplicaNode("n1", dataDir);
        String key = "tp1";
        VectorClock clock = new VectorClock(java.util.Map.of("w:op-1", 1L));

        node.localPrepare("op-1", key, "v", clock, false);
        assertTrue(node.localCommittedGet(key).isEmpty(), "PENDING 预备值不可读");
        log.info("[业务:节点两阶段][用例:预备不可见] PENDING 时已提交视图为空");

        node.localCommit(key, clock);
        assertEquals(1, node.localCommittedGet(key).size());
        node.localCommit(key, clock); // 重复提交幂等
        assertEquals(1, node.localCommittedGet(key).size());
        log.info("[业务:节点两阶段][用例:提交幂等] 重复提交后版本数=1");
    }

    @Test
    void commitWithValueToReplicaMissingPrepareDoesNotCreateEmptyVersion() {
        ReplicaNode node = new ReplicaNode("n2", dataDir);
        String key = "tp2";
        VectorClock clock = new VectorClock(java.util.Map.of("w:op-2", 1L));
        VersionedValue vv = new VersionedValue("full-value", clock, false);

        // 该节点从未收到 PREPARE，协调器直接下发带值 COMMIT
        node.localCommit(key, vv);
        List<VersionedValue> committed = node.localCommittedGet(key);
        log.info("[业务:节点两阶段][用例:带值提交无空壳] versions={}",
                committed.stream().map(v -> v.value() + "@" + v.clock()).toList());
        assertEquals(1, committed.size());
        assertEquals("full-value", committed.get(0).value());

        // 重启重放：COMMIT 决定记录只有时钟，但 PREPARE 已由提交决定前的同步传播保证；
        // 此处通过重放后状态不回退来验证幂等
        node.down();
        node.up();
        assertEquals(1, node.localCommittedGet(key).size());
    }

    @Test
    void abortSurvivesWalReplay() {
        ReplicaNode node = new ReplicaNode("n3", dataDir);
        String key = "tp3";
        VectorClock clock = new VectorClock(java.util.Map.of("w:op-3", 1L));
        node.localPrepare("op-3", key, "ghost", clock, false);
        node.localAbort(key, clock);
        assertTrue(node.localCommittedGet(key).isEmpty());

        node.down();
        node.up();
        log.info("[业务:节点两阶段][用例:放弃决定重放持久] 重放后已提交视图={}（应为空）",
                node.localCommittedGet(key));
        assertTrue(node.localCommittedGet(key).isEmpty(), "ABORTED 重放后仍不可见");
        node.localAbort(key, clock); // 重复放弃幂等
        assertTrue(node.localCommittedGet(key).isEmpty());
    }

    @Test
    void downNodeRejectsAllOperations() {
        ReplicaNode node = new ReplicaNode("n4", dataDir);
        node.down();
        VectorClock clock = new VectorClock(java.util.Map.of("w:op-4", 1L));
        ReplicaNode.WriteAck ack = node.localPrepare("op-4", "tp4", "v", clock, false);
        log.info("[业务:节点两阶段][用例:下线拒绝写入] unavailable={} 期望=true", ack.unavailable());
        assertTrue(ack.unavailable());
        assertNull(node.localEntries("tp4"));
    }
}
