package com.github.feng111627.quorumreplicastorejava.node;

import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;

import java.nio.file.Path;
import java.util.*;

/**
 * 单个副本节点：内存表 + 本地 WAL 落盘，可标记上下线模拟宕机/分区。
 * 每个键保存一组兄弟版本（并发写冲突时多于一个）。
 */
public class ReplicaNode {
    private final String nodeId;
    private final Path walFile;

    private final Map<String, List<VersionedValue>> store = new HashMap<>();
    private final Set<String> processedOps = new HashSet<>();
    private Wal wal;
    private volatile boolean up;

    public ReplicaNode(String nodeId, Path dataDir) {
        this.nodeId = nodeId;
        this.walFile = dataDir.resolve(nodeId).resolve("wal.log");
        up(); // 初始为上线状态
    }

    public String id() {
        return nodeId;
    }

    /** 上线（模拟重启恢复：清空内存，重放 WAL 回到崩溃前已提交状态）。 */
    public synchronized void up() {
        store.clear();
        processedOps.clear();
        Wal.replay(walFile, this::applyReplay, processedOps::add);
        this.wal = new Wal(walFile);
        this.up = true;
    }

    /** 下线（模拟宕机/分区：拒绝一切读写，内存状态丢失，WAL 保留在盘上）。 */
    public synchronized void down() {
        this.up = false;
        if (wal != null) wal.close();
        wal = null;
        store.clear();
        processedOps.clear();
    }

    public boolean isUp() {
        return up;
    }

    /** 本地读：返回该副本上某键的全部兄弟版本；节点下线返回 null（视为无响应）。 */
    public synchronized List<VersionedValue> localGet(String key) {
        if (!up) return null;
        List<VersionedValue> v = store.get(key);
        return v == null ? List.of() : List.copyOf(v);
    }

    /**
     * 本地写：基于客户端读上下文做冲突检测与过期拒绝。
     * opId 幂等：同一 opId 重放/重复提交不会产生重复效果。
     *
     * @param context  客户端读到的上下文（为空表示无依据的盲写，按全新写处理）
     * @param newClock 协调器为本次写分配的时钟（context 递增写标记得到）
     */
    public synchronized WriteAck localPut(String opId, String key, String value,
                                          VectorClock context, VectorClock newClock,
                                          boolean tombstone) {
        if (!up) return WriteAck.down();
        if (processedOps.contains(opId)) return WriteAck.dup();

        List<VersionedValue> existing = store.getOrDefault(key, List.of());
        List<VersionedValue> kept = new ArrayList<>();
        for (VersionedValue sib : existing) {
            // 用本次写的新时钟与本地版本比较：
            // 本地版本更新 -> 本次写过期，拒绝，防止旧数据覆盖已提交的新数据；
            // 互不可比 -> 并发写，保留为兄弟版本，读时暴露冲突。
            switch (VectorClock.compare(sib.clock(), newClock)) {
                case AFTER -> {
                    return WriteAck.rejectedStale();
                }
                case CONCURRENT -> kept.add(sib);
                case BEFORE, EQUAL -> { /* 被本次写取代 */ }
            }
        }
        kept.add(new VersionedValue(value, newClock, tombstone));
        wal.append(opId, key, new VersionedValue(value, newClock, tombstone));
        store.put(key, kept);
        processedOps.add(opId);
        return WriteAck.ok();
    }

    /** 读修复：把协调器归并后的版本集合并入本地（去掉被支配的旧版本，补上缺失的新版本）。 */
    public synchronized void localMerge(String key, List<VersionedValue> versions, String repairOpId) {
        if (!up) return;
        List<VersionedValue> union = new ArrayList<>(store.getOrDefault(key, List.of()));
        for (VersionedValue v : versions) {
            boolean dominated = false;
            Iterator<VersionedValue> it = union.iterator();
            while (it.hasNext()) {
                VersionedValue u = it.next();
                switch (VectorClock.compare(u.clock(), v.clock())) {
                    case BEFORE -> it.remove();      // 本地旧版本被取代
                    case EQUAL, AFTER -> dominated = true; // 本地已有同等或更新版本
                    case CONCURRENT -> { /* 都保留 */ }
                }
            }
            if (!dominated) union.add(v);
        }
        store.put(key, union);
        for (VersionedValue v : versions) {
            wal.append(repairOpId, key, v);
        }
    }

    /** WAL 重放：与 localMerge 相同的归并语义，保证重放幂等、不产生版本错乱。 */
    private void applyReplay(String key, VersionedValue v) {
        List<VersionedValue> union = new ArrayList<>(store.getOrDefault(key, List.of()));
        boolean dominated = false;
        Iterator<VersionedValue> it = union.iterator();
        while (it.hasNext()) {
            VersionedValue u = it.next();
            switch (VectorClock.compare(u.clock(), v.clock())) {
                case BEFORE -> it.remove();
                case EQUAL, AFTER -> dominated = true;
                case CONCURRENT -> { }
            }
        }
        if (!dominated) union.add(v);
        store.put(key, union);
    }

    public record WriteAck(boolean accepted, boolean stale, boolean duplicate, boolean unavailable) {
        public static WriteAck ok()            { return new WriteAck(true, false, false, false); }
        public static WriteAck rejectedStale() { return new WriteAck(false, true, false, false); }
        public static WriteAck dup()           { return new WriteAck(true, false, true, false); }
        public static WriteAck down()          { return new WriteAck(false, false, false, true); }
    }
}
