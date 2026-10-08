package com.github.feng111627.quorumreplicastorejava.node;

import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;

import java.nio.file.Path;
import java.util.*;

/**
 * 单个副本节点：内存表 + 本地 WAL 落盘，可标记上下线模拟宕机/分区。
 *
 * 写入采用两阶段：
 *   1. prepare（localPut）：值进入 pending 区，对读不可见；
 *   2. commit（commitOp）：达到法定人数后把 pending 版本并入已提交表；
 *      abort（abortOp）：未达法定人数时撤销 pending，当作从未发生。
 * 已提交表（store）与未提交区（pending）分离，读路径只看已提交表，
 * 因此未凑够确认的写永远不会被读到，也不会被读修复传播。
 */
public class ReplicaNode {
    /** 未提交版本：prepare 阶段落下的值，带着产生它的 opId，提交前对读不可见。 */
    public record PendingEntry(String opId, VersionedValue value) {}

    /** 某键在本副本上的完整视图（巡检用只读快照）：已提交版本 + 未提交残留。 */
    public record Snapshot(List<VersionedValue> committed, List<PendingEntry> pending) {}

    private final String nodeId;
    private final Path walFile;

    private final Map<String, List<VersionedValue>> store = new HashMap<>();  // 已提交版本
    private final Map<String, List<PendingEntry>> pending = new HashMap<>();  // 未提交版本
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

    /** 上线（模拟重启恢复：清空内存，重放 WAL 回到崩溃前状态）。 */
    public synchronized void up() {
        store.clear();
        pending.clear();
        processedOps.clear();
        Wal.replay(walFile, new Wal.ReplayHandler() {
            @Override
            public void onPut(String opId, String key, VersionedValue v, boolean committed) {
                processedOps.add(opId);
                if (committed) {
                    applyMergeCommitted(key, v);
                } else {
                    pending.computeIfAbsent(key, k -> new ArrayList<>())
                            .add(new PendingEntry(opId, v));
                }
            }

            @Override
            public void onCommit(String opId, String key) {
                applyCommit(opId, key);
            }

            @Override
            public void onAbort(String opId, String key) {
                applyAbort(opId, key);
            }
        });
        this.wal = new Wal(walFile);
        this.up = true;
    }

    /** 下线（模拟宕机/分区：拒绝一切读写，内存状态丢失，WAL 保留在盘上）。 */
    public synchronized void down() {
        this.up = false;
        if (wal != null) wal.close();
        wal = null;
        store.clear();
        pending.clear();
        processedOps.clear();
    }

    public boolean isUp() {
        return up;
    }

    /** 本地读：只返回已提交版本；未提交的 pending 对读不可见。节点下线返回 null。 */
    public synchronized List<VersionedValue> localGet(String key) {
        if (!up) return null;
        List<VersionedValue> v = store.get(key);
        return v == null ? List.of() : List.copyOf(v);
    }

    /** 巡检用只读快照：已提交版本 + 未提交残留。节点下线返回 null。 */
    public synchronized Snapshot localInspect(String key) {
        if (!up) return null;
        return new Snapshot(
                List.copyOf(store.getOrDefault(key, List.of())),
                List.copyOf(pending.getOrDefault(key, List.of())));
    }

    /** 本副本上出现过（已提交或未提交）的全部键。节点下线返回 null。 */
    public synchronized Set<String> localKeys() {
        if (!up) return null;
        Set<String> keys = new HashSet<>(store.keySet());
        keys.addAll(pending.keySet());
        return keys;
    }

    /**
     * 两阶段写的第一阶段（prepare）：基于客户端读上下文做冲突检测与过期拒绝，
     * 通过后把值写入 pending 区（对读不可见），等待协调器的 commit/abort 决定。
     * opId 幂等：同一 opId 重复 prepare 直接视为已接受。
     *
     * @param context  客户端读到的上下文（为空表示无依据的盲写，按全新写处理）
     * @param newClock 协调器为本次写分配的时钟（context 递增写标记得到）
     */
    public synchronized WriteAck localPut(String opId, String key, String value,
                                          VectorClock context, VectorClock newClock,
                                          boolean tombstone) {
        if (!up) return WriteAck.down();
        if (processedOps.contains(opId)) return WriteAck.dup();

        // 过期检查只针对已提交版本：本地有更新的已提交版本时拒绝本次写，
        // 防止旧数据覆盖已提交的新数据；pending 不构成拒绝依据（它从未生效）。
        for (VersionedValue sib : store.getOrDefault(key, List.of())) {
            if (VectorClock.compare(sib.clock(), newClock) == VectorClock.Relation.AFTER) {
                return WriteAck.rejectedStale();
            }
        }
        VersionedValue v = new VersionedValue(value, newClock, tombstone);
        wal.appendPut(opId, key, v, false);
        pending.computeIfAbsent(key, k -> new ArrayList<>()).add(new PendingEntry(opId, v));
        processedOps.add(opId);
        return WriteAck.ok();
    }

    /**
     * 两阶段写的提交：把该 opId 的 pending 版本并入已提交表（归并语义）。
     * 幂等：没有对应 pending 时为空操作。返回是否有状态变化。
     */
    public synchronized boolean commitOp(String opId, String key) {
        if (!up) return false;
        boolean changed = applyCommit(opId, key);
        if (changed) wal.appendCommit(opId, key);
        return changed;
    }

    /**
     * 两阶段写的撤销：删除该 opId 的 pending 版本，当作从未发生。
     * 同时解除 opId 的已处理标记，允许客户端用同一 opId 干净地重试。
     * 幂等：没有对应 pending 时为空操作。返回是否有状态变化。
     */
    public synchronized boolean abortOp(String opId, String key) {
        if (!up) return false;
        boolean changed = applyAbort(opId, key);
        if (changed) wal.appendAbort(opId, key);
        return changed;
    }

    /**
     * 读修复/收敛：把归并后的已提交版本集合并入本地已提交表；
     * 被已提交版本支配（或相同）的 pending 残留一并丢弃。
     * 幂等：归并语义下重复合并结果相同；无变化时不写 WAL。返回是否有状态变化。
     */
    public synchronized boolean localMerge(String key, List<VersionedValue> versions, String repairOpId) {
        if (!up) return false;
        boolean changed = false;
        for (VersionedValue v : versions) {
            if (applyMergeCommitted(key, v)) {
                wal.appendPut(repairOpId, key, v, true);
                changed = true;
            }
        }
        return changed;
    }

    /** 提交状态转移（运行时的 commitOp 与重启时的 WAL 重放共用，保证语义一致）。 */
    private boolean applyCommit(String opId, String key) {
        List<PendingEntry> pend = pending.get(key);
        if (pend == null) return false;
        PendingEntry target = null;
        for (PendingEntry p : pend) {
            if (p.opId().equals(opId)) {
                target = p;
                break;
            }
        }
        if (target == null) return false;
        pend.remove(target);
        if (pend.isEmpty()) pending.remove(key);
        // 并入已提交表；若本地已有支配它的已提交版本（防御性，正常流程不会发生），自然丢弃
        applyMergeCommitted(key, target.value());
        return true;
    }

    /** 撤销状态转移（运行时的 abortOp 与重启时的 WAL 重放共用）。 */
    private boolean applyAbort(String opId, String key) {
        List<PendingEntry> pend = pending.get(key);
        if (pend == null) return false;
        boolean removed = pend.removeIf(p -> p.opId().equals(opId));
        if (!removed) return false;
        if (pend.isEmpty()) pending.remove(key);
        processedOps.remove(opId); // 允许客户端用同一 opId 干净重试
        return true;
    }

    /**
     * 已提交归并（localMerge、applyCommit、WAL 重放共用）：
     * 去掉被支配的旧版本，保留并发兄弟；并丢弃被该已提交版本支配（或相同）的 pending 残留。
     */
    private boolean applyMergeCommitted(String key, VersionedValue v) {
        List<VersionedValue> union = new ArrayList<>(store.getOrDefault(key, List.of()));
        boolean dominated = false;
        Iterator<VersionedValue> it = union.iterator();
        while (it.hasNext()) {
            VersionedValue u = it.next();
            switch (VectorClock.compare(u.clock(), v.clock())) {
                case BEFORE -> it.remove();          // 本地旧版本被取代
                case EQUAL, AFTER -> dominated = true; // 本地已有同等或更新版本
                case CONCURRENT -> { /* 都保留为兄弟 */ }
            }
        }
        if (dominated) return false;
        union.add(v);
        store.put(key, union);
        dropDominatedPending(key, List.of(v));
        return true;
    }

    /** 丢弃被给定已提交版本支配（或相同）的 pending 残留。 */
    private void dropDominatedPending(String key, List<VersionedValue> committedVersions) {
        List<PendingEntry> pend = pending.get(key);
        if (pend == null) return;
        pend.removeIf(p -> committedVersions.stream().anyMatch(v -> {
            var rel = VectorClock.compare(v.clock(), p.value().clock());
            return rel == VectorClock.Relation.AFTER || rel == VectorClock.Relation.EQUAL;
        }));
        if (pend.isEmpty()) pending.remove(key);
    }

    public record WriteAck(boolean accepted, boolean stale, boolean duplicate, boolean unavailable) {
        public static WriteAck ok()            { return new WriteAck(true, false, false, false); }
        public static WriteAck rejectedStale() { return new WriteAck(false, true, false, false); }
        public static WriteAck dup()           { return new WriteAck(true, false, true, false); }
        public static WriteAck down()          { return new WriteAck(false, false, false, true); }
    }
}
