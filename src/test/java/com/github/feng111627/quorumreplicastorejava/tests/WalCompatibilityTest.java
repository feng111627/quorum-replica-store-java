package com.github.feng111627.quorumreplicastorejava.tests;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.Status;
import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import com.github.feng111627.quorumreplicastorejava.node.Wal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 业务场景：WAL 格式兼容。
 * 升级前落盘的旧格式日志（无 QRS1 文件头）在新版本启动时必须仍能读取，
 * 且旧记录一律视为已提交数据——老版本写过的值不会因升级而丢或变成未提交残留。
 */
class WalCompatibilityTest {
    private static final Logger log = LoggerFactory.getLogger(WalCompatibilityTest.class);
    private static final List<String> NODES = List.of("n1", "n2", "n3", "n4", "n5");

    /** 用旧版本布局手工写一条旧格式记录：opId,key,tombstone,hasValue,value?,clock。 */
    private static void writeLegacyRecord(Path file, String opId, String key, VersionedValue v)
            throws IOException {
        Files.createDirectories(file.getParent());
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(file.toFile(), true)))) {
            out.writeUTF(opId);
            out.writeUTF(key);
            out.writeBoolean(v.tombstone());
            out.writeBoolean(v.value() != null);
            if (v.value() != null) out.writeUTF(v.value());
            Map<String, Long> entries = v.clock().entries();
            out.writeInt(entries.size());
            for (var e : entries.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeLong(e.getValue());
            }
        }
    }

    @Test
    void legacyWalIsReplayedAsCommittedData() throws IOException {
        Path dir = Files.createTempDirectory("wal-legacy");
        String key = "legacy-key";
        // 用纯路由（不创建节点、不写文件头）算出该键的副本，把旧记录写到每个副本的 wal.log
        List<String> replicas =
                new com.github.feng111627.quorumreplicastorejava.core.ShardRouter(3, NODES, 3)
                        .replicasOf(key);
        VectorClock clock = new VectorClock(Map.of("w:old-op", 1L));
        for (String nodeId : replicas) {
            writeLegacyRecord(dir.resolve(nodeId).resolve("wal.log"),
                    "old-op", key, new VersionedValue("legacy-value", clock, false));
        }

        // 新版本打开同一份数据目录：旧数据应被当成已提交数据读出
        NodeRegistry registry = new NodeRegistry(NODES, 3, 3, dir);
        Coordinator coordinator = new Coordinator(registry, 2, 2, dir);
        ReadResult g = coordinator.get(key);
        log.info("[业务:WAL格式兼容][用例:旧日志按已提交恢复] read={} value={} 期望=OK/legacy-value",
                g.status(), g.values().isEmpty() ? null : g.values().get(0).value());
        assertEquals(Status.OK, g.status());
        assertEquals("legacy-value", g.values().get(0).value());

        // 巡检不得把旧数据误判成未提交残留
        assertTrue(coordinator.inspectKey(key).consistent());
        log.info("[业务:WAL格式兼容][用例:旧日志按已提交恢复] 巡检 consistent=true（旧数据不是残留）");
    }
}
