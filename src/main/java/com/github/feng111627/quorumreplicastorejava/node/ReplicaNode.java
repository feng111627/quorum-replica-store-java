package com.github.feng111627.quorumreplicastorejava.node;

import com.github.feng111627.quorumreplicastorejava.core.EntryState;
import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;

import java.nio.file.Path;
import java.util.*;

/**
 * 单个副本节点：内存表 + 本地 WAL 落盘，可标记上下线模拟宕机/分区。
 *
 * 每个键保存一组带提交状态的版本（{@link StoredEntry}）：
 * <ul>
 *   <li>PENDING：协调器两阶段写的预备值，凑够法定人数前读路径绝不返回；</li>
 *   <li>COMMITTED：收到提交决定的版本，是唯一可读、可被读修复传播的数据；</li>
 *   <li>ABORTED：收到放弃决定的版本，永不返回，只留标记防止旧值复活。</li>
 * </ul>
 * 没凑够确认的写最终一定收到 ABORTED 决定：节点重上线或整机重启（重放 WAL）后
 * 也不会把残留预备值当数据冒出来。
 */
public class ReplicaNode {
    private final String nodeId;
    private final Path walFile;

    private final Map<String, List<StoredEntry>> store = new HashMap<>();
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

    /** 上线（模拟重启恢复：清空内存，重放 WAL 回到崩溃前状态，含提交/放弃决定）。 */
    public synchronized void up() {
        store.clear();
        processedOps.clear();
        Wal.replayV2(walFile, (type, opId, commit, key, value) -> {
            switch (type) {
                case Wal.TYPE_LEGACY_COMMITTED -> {
                    // 旧格式日志没有两阶段记录，全部是已确认数据
                    processedOps.add(opId);
                    putCommittedMerge(key, value);
                }
                case Wal.TYPE_PREPARE -> {
                    if (opId != null) processedOps.add(opId);
                    putEntry(key, StoredEntry.pending(value));
                }
                case Wal.TYPE_COMMIT -> applyCommitRecord(key, value);
                case Wal.TYPE_ABORT -> applyAbort(key, value.clock());
                case Wal.TYPE_GC_ABORT -> purgeByClock(key, value.clock());
                default -> throw new IllegalStateException("replica wal cannot hold type " + type);
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
        processedOps.clear();
    }

    public boolean isUp() {
        return up;
    }

    /**
     * 本地读（管理/调试用）：返回该副本上某键的全部版本及其提交状态；
     * 节点下线返回 null（视为无响应）。
     */
    public synchronized List<StoredEntry> localEntries(String key) {
        if (!up) return null;
        return List.copyOf(store.getOrDefault(key, List.of()));
    }

    /** 读路径专用：只返回已提交的兄弟版本；PENDING/ABORTED 一律不可见。节点下线返回 null。 */
    public synchronized List<VersionedValue> localCommittedGet(String key) {
        if (!up) return null;
        return store.getOrDefault(key, List.of()).stream()
                .filter(e -> e.state() == EntryState.COMMITTED)
                .map(StoredEntry::version)
                .toList();
    }

    /** 枚举本地有版本记录（含 PENDING/ABORTED）的全部键；节点下线返回 null。 */
    public synchronized Set<String> localKeys() {
        if (!up) return null;
        return Set.copyOf(store.keySet());
    }

    /**
     * 两阶段写·预备：落盘 PENDING 预备值，基于已提交版本做冲突检测与过期拒绝。
     * 预备阶段不取代任何本地版本（含旧的已提交版本），只有提交决定到达时才归并清理。
     * opId 幂等：同一 opId 重放/重复提交不会产生重复效果。
     */
    public synchronized WriteAck localPrepare(String opId, String key, String value,
                                              VectorClock newClock, boolean tombstone) {
        if (!up) return WriteAck.down();
        if (processedOps.contains(opId)) return WriteAck.dup();

        for (StoredEntry e : store.getOrDefault(key, List.of())) {
            if (e.state() != EntryState.COMMITTED) continue;
            // 只用已提交版本判定：本地已提交版本更新 -> 本次写过期，拒绝覆盖已提交新数据
            if (VectorClock.compare(e.version().clock(), newClock) == VectorClock.Relation.AFTER) {
                return WriteAck.rejectedStale();
            }
        }
        VersionedValue vv = new VersionedValue(value, newClock, tombstone);
        wal.appendPrepare(opId, key, vv);
        processedOps.add(opId);
        putEntry(key, StoredEntry.pending(vv));
        return WriteAck.ok();
    }

    /**
     * 两阶段写·提交（仅时钟）：副本本地已持有预备值、只需迁移状态时使用。
     */
    public synchronized void localCommit(String key, VectorClock clock) {
        if (!up) return;
        if (applyCommit(key, clock)) {
            // 找到已存储的版本（含值）写入完整 COMMIT 记录；找不到则只能写时钟
            VersionedValue stored = findVersion(key, clock);
            if (stored != null) wal.appendCommit(key, stored);
        }
    }

    /**
     * 两阶段写·提交（带完整值）：把预备值转为已提交；若该副本此前没收到预备值
     * （如宕机错过阶段一），也直接并入这条已提交版本。决定幂等。
     */
    public synchronized void localCommit(String key, VersionedValue version) {
        if (!up) return;
        VectorClock clock = version.clock();
        boolean changed;
        List<StoredEntry> entries = store.get(key);
        if (entries != null && entries.stream().anyMatch(e -> e.version().clock().equals(clock))) {
            changed = applyCommit(key, clock);
        } else {
            changed = mergeOneCommitted(key, version);
        }
        if (changed) {
            // 始终落完整版本：错过 PREPARE 的副本重启后也能凭 COMMIT 记录重建
            wal.appendCommit(key, version);
        }
    }

    /** 两阶段写·放弃：该时钟的版本标记 ABORTED（永不返回）。决定幂等。 */
    public synchronized void localAbort(String key, VectorClock clock) {
        if (!up) return;
        if (applyAbort(key, clock)) {
            wal.appendAbort(key, clock);
        }
    }

    /**
     * 读修复/收敛：并入一组已提交版本（去掉被支配旧版本，补上缺失新版本，保留并发兄弟）。
     * 只处理 COMMITTED，绝不把 PENDING 提升为可读数据。
     *
     * @return 是否实际发生改动（用于避免重复落盘无意义的修复记录）
     */
    public synchronized boolean localMergeCommitted(String key, List<VersionedValue> versions, String repairOpId) {
        if (!up) return false;
        List<VersionedValue> applied = new ArrayList<>();
        for (VersionedValue v : versions) {
            if (mergeOneCommitted(key, v)) applied.add(v);
        }
        // 修复传播的是完整已提交版本，落 COMMIT 记录，错过 PREPARE 的副本重启后也能重建
        for (VersionedValue v : applied) {
            wal.appendCommit(key, v);
        }
        return !applied.isEmpty();
    }

    /**
     * 收敛：按放弃决定物理清理本地对应残留（PENDING/ABORTED 记录本身）。
     * 每个被清理的时钟追加一条 GC 记录并 fsync：重启重放后同样保持已回收，巡检/收敛结果稳定。
     */
    public synchronized boolean localPurgeAborted(String key, List<VectorClock> clocks) {
        if (!up) return false;
        boolean changed = false;
        for (VectorClock c : clocks) {
            List<StoredEntry> entries = store.get(key);
            if (entries == null) continue;
            if (entries.removeIf(e -> e.version().clock().equals(c))) {
                wal.appendGcAbort(key, c);
                changed = true;
            }
        }
        return changed;
    }

    /** 重放 GC 记录：该时钟的任何状态记录都已在崩溃前回收，重放时直接不重建。 */
    private void purgeByClock(String key, VectorClock clock) {
        List<StoredEntry> entries = store.get(key);
        if (entries != null) entries.removeIf(e -> e.version().clock().equals(clock));
    }

    // ---- 内部归并原语（不加锁、不落盘，供 WAL 重放复用） ----

    private void putEntry(String key, StoredEntry entry) {
        List<StoredEntry> entries = store.computeIfAbsent(key, k -> new ArrayList<>());
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).version().clock().equals(entry.version().clock())) {
                // 相同时钟：状态只向前迁移 PENDING -> COMMITTED/ABORTED，值相同则不重复
                entries.set(i, mergeSameClock(entries.get(i), entry));
                return;
            }
        }
        entries.add(entry);
    }

    private StoredEntry mergeSameClock(StoredEntry oldEntry, StoredEntry incoming) {
        if (oldEntry.state() == EntryState.COMMITTED || oldEntry.state() == EntryState.ABORTED) {
            return oldEntry; // 已终态，不被预备记录覆盖
        }
        return incoming;
    }

    private void putCommittedMerge(String key, VersionedValue v) {
        mergeOneCommitted(key, v);
    }

    /**
     * 把一个已提交版本并入本地：
     * 相同/更新的已提交版本存在 -> 无改动；旧已提交版本被支配 -> 移除；并发兄弟 -> 保留。
     * 相同时钟但当前为 PENDING/ABORTED -> 迁移为 COMMITTED。
     */
    private boolean mergeOneCommitted(String key, VersionedValue v) {
        List<StoredEntry> entries = store.computeIfAbsent(key, k -> new ArrayList<>());
        for (int i = 0; i < entries.size(); i++) {
            StoredEntry e = entries.get(i);
            if (e.version().clock().equals(v.clock())) {
                if (e.state() == EntryState.COMMITTED) return false;
                entries.set(i, StoredEntry.committed(v));
                pruneCommittedDominatedBy(key, v.clock());
                return true;
            }
            if (e.state() == EntryState.COMMITTED) {
                switch (VectorClock.compare(e.version().clock(), v.clock())) {
                    case EQUAL, AFTER -> { return false; }
                    case BEFORE -> { /* 本地旧已提交版本稍后被 prune 移除 */ }
                    case CONCURRENT -> { /* 并发兄弟都保留 */ }
                }
            }
        }
        entries.add(StoredEntry.committed(v));
        pruneCommittedDominatedBy(key, v.clock());
        return true;
    }

    /** 去掉被指定时钟支配的旧已提交版本；PENDING/ABORTED 不动（由提交/放弃决定各自处理）。 */
    private void pruneCommittedDominatedBy(String key, VectorClock clock) {
        List<StoredEntry> entries = store.get(key);
        if (entries == null) return;
        entries.removeIf(e -> e.state() == EntryState.COMMITTED
                && VectorClock.compare(e.version().clock(), clock) == VectorClock.Relation.BEFORE);
    }

    /** @return 是否发生状态迁移（用于决定是否再落一条决定记录） */
    private boolean applyCommit(String key, VectorClock clock) {
        List<StoredEntry> entries = store.get(key);
        if (entries == null) return false;
        boolean moved = false;
        for (int i = 0; i < entries.size(); i++) {
            StoredEntry e = entries.get(i);
            if (!e.version().clock().equals(clock)) continue;
            if (e.state() == EntryState.COMMITTED) return false;
            entries.set(i, e.withState(EntryState.COMMITTED));
            moved = true;
        }
        if (moved) pruneCommittedDominatedBy(key, clock);
        return moved;
    }

    /** 重放 COMMIT 记录：有对应预备值则迁移状态；没有（错过 PREPARE）则按完整值直接并入。 */
    private void applyCommitRecord(String key, VersionedValue version) {
        List<StoredEntry> entries = store.get(key);
        boolean hasClock = entries != null
                && entries.stream().anyMatch(e -> e.version().clock().equals(version.clock()));
        if (hasClock) {
            applyCommit(key, version.clock());
        } else {
            mergeOneCommitted(key, version);
        }
    }

    private VersionedValue findVersion(String key, VectorClock clock) {
        List<StoredEntry> entries = store.get(key);
        if (entries == null) return null;
        return entries.stream()
                .filter(e -> e.version().clock().equals(clock))
                .map(StoredEntry::version)
                .findFirst().orElse(null);
    }

    /** @return 是否发生状态迁移。 */
    private boolean applyAbort(String key, VectorClock clock) {
        List<StoredEntry> entries = store.get(key);
        if (entries == null) return false;
        boolean moved = false;
        for (int i = 0; i < entries.size(); i++) {
            StoredEntry e = entries.get(i);
            if (!e.version().clock().equals(clock)) continue;
            if (e.state() == EntryState.ABORTED) return false;
            entries.set(i, e.withState(EntryState.ABORTED));
            moved = true;
        }
        return moved;
    }

    public record WriteAck(boolean accepted, boolean stale, boolean duplicate, boolean unavailable) {
        public static WriteAck ok()            { return new WriteAck(true, false, false, false); }
        public static WriteAck rejectedStale() { return new WriteAck(false, true, false, false); }
        public static WriteAck dup()           { return new WriteAck(true, false, true, false); }
        public static WriteAck down()          { return new WriteAck(false, false, false, true); }
    }
}
