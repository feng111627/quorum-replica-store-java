package com.github.feng111627.quorumreplicastorejava.cluster;

import com.github.feng111627.quorumreplicastorejava.core.EntryState;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.ClusterReport;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.DecisionOutcome;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.KeyReport;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.RepairResult;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.ReplicaDiff;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.VersionView;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.DiffKind;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.ValueView;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import com.github.feng111627.quorumreplicastorejava.node.StoredEntry;
import com.github.feng111627.quorumreplicastorejava.node.Wal;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * 协调器：两阶段 quorum 写、只读 quorum 读 + 读修复、只读一致性巡检、按需幂等收敛。
 *
 * 写流程：先在各副本落 PENDING 预备值；凑够 W 个确认后，先把 COMMIT 决定 fsync 到
 * 协调器决定日志，再向副本下发提交。凑不够则先 fsync ABORT 决定，再让所有在线副本放弃，
 * 没确认的写对读路径完全不可见，节点重上线/整机重启后也不会复活（决定日志与副本 WAL 双保险）。
 */
public class Coordinator {
    private final NodeRegistry registry;
    private final int writeQuorum;
    private final int readQuorum;

    /** 已 fsync 的提交/放弃决定：版本时钟 -> 决定（整机重启后由决定日志恢复）。 */
    private final Map<VectorClock, Decision> decisions = new LinkedHashMap<>();
    private Wal decisionLog;

    /** 一次写的最终决定。tombstone 仅在 commit 时有意义（删除标记，重启后仍要正确呈现 NOT_FOUND）。 */
    public record Decision(String key, boolean commit, boolean tombstone) {}

    public Coordinator(NodeRegistry registry, int writeQuorum, int readQuorum) {
        this(registry, writeQuorum, readQuorum, null);
    }

    public Coordinator(NodeRegistry registry, int writeQuorum, int readQuorum, Path dataRoot) {
        this.registry = registry;
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
        if (dataRoot != null) {
            Path logFile = dataRoot.resolve("coordinator-decision.log");
            Wal.replayV2(logFile, (type, opId, commit, key, value) -> {
                if (type == Wal.TYPE_DECISION) {
                    decisions.put(value.clock(),
                            new Decision(key, commit, value.tombstone()));
                }
            });
            this.decisionLog = new Wal(logFile);
        }
    }

    public WriteResult put(String opId, String key, String value, VectorClock context) {
        return write(opId, key, value, context, false);
    }

    public WriteResult delete(String opId, String key, VectorClock context) {
        return write(opId, key, null, context, true);
    }

    private WriteResult write(String opId, String key, String value, VectorClock context, boolean tombstone) {
        // 为本次写分配时钟：在客户端读上下文上加唯一写标记，
        // 并发写（相同上下文、不同 opId）会得到互不可比的时钟，从而被识别为冲突。
        VectorClock newClock = (context == null ? new VectorClock() : context).copy();
        newClock.increment("w:" + opId);

        // 客户端用同一 opId 重试：时钟确定相同，直接返回此前已落盘的决定，保证幂等且不复活已放弃写。
        Decision prior = decisions.get(newClock);
        if (prior != null) {
            return prior.commit() ? WriteResult.ok(newClock) : WriteResult.quorumNotMet(0, writeQuorum);
        }

        // 阶段一：各副本落 PENDING 预备值（不取代任何本地已提交版本）
        int accepted = 0, stale = 0;
        for (ReplicaNode node : registry.replicasFor(key)) {
            ReplicaNode.WriteAck ack = node.localPrepare(opId, key, value, newClock, tombstone);
            if (ack.accepted()) accepted++;
            else if (ack.stale()) stale++;
        }

        boolean commit = accepted >= writeQuorum;
        // 决定先 fsync 落盘（存在决定日志时），再对外动作：保证整机重启后决定不丢
        recordDecision(commit, tombstone, key, newClock);

        if (commit) {
            // 阶段二：向所有在线副本下发提交（带完整值：已预备的转 COMMITTED，错过预备的也直接并入已提交版本）
            VersionedValue committed = new VersionedValue(value, newClock, tombstone);
            for (ReplicaNode node : registry.replicasFor(key)) {
                node.localCommit(key, committed);
            }
            return WriteResult.ok(newClock);
        }

        // 失败收场：放弃决定下发到所有在线副本，残留预备值转 ABORTED、永不返回
        for (ReplicaNode node : registry.replicasFor(key)) {
            node.localAbort(key, newClock);
        }
        return stale > 0 ? WriteResult.staleClock() : WriteResult.quorumNotMet(accepted, writeQuorum);
    }

    private void recordDecision(boolean commit, boolean tombstone, String key, VectorClock clock) {
        decisions.put(clock, new Decision(key, commit, tombstone));
        if (decisionLog != null) {
            decisionLog.appendGlobalDecision(commit, tombstone, key, clock);
        }
    }

    public ReadResult get(String key) {
        List<ReplicaNode> replicas = registry.replicasFor(key);
        List<VersionedValue> all = new ArrayList<>();
        int responded = 0;
        List<ReplicaNode> upNodes = new ArrayList<>();
        for (ReplicaNode node : replicas) {
            List<VersionedValue> local = node.localCommittedGet(key);
            if (local == null) continue; // 节点下线，无响应
            responded++;
            upNodes.add(node);
            all.addAll(local);
        }
        if (responded < readQuorum) {
            return ReadResult.quorumNotMet(responded, readQuorum);
        }

        // 读路径只归并已提交版本；PENDING（未凑够确认的写）绝不参与，也不会被读修复传播
        List<VersionedValue> reconciled = reconcile(all);
        VectorClock readContext = mergeAll(all);

        // 读修复：把已提交归并结果回写到在线副本，避免已提交数据在副本切换后回退
        if (!reconciled.isEmpty()) {
            String repairOp = "repair:" + UUID.randomUUID();
            for (ReplicaNode node : upNodes) {
                node.localMergeCommitted(key, reconciled, repairOp);
            }
        }

        List<VersionedValue> live = reconciled.stream().filter(v -> !v.tombstone()).toList();
        if (live.isEmpty()) {
            return ReadResult.notFound(readContext);
        }
        if (live.size() > 1) {
            return ReadResult.conflict(live.stream().map(ValueView::of).toList(), readContext);
        }
        return ReadResult.ok(ValueView.of(live.get(0)), readContext);
    }

    // ---- 只读一致性巡检 ----

    /** 只读巡检单个键：收集全部副本（含下线副本）的版本与提交状态，不改动任何数据。 */
    public KeyReport inspectKey(String key) {
        List<ReplicaNode> replicas = registry.replicasFor(key);

        // 集群视角的已提交版本：各在线副本 COMMITTED 版本 + 决定日志里 commit 的版本
        List<VersionedValue> committed = new ArrayList<>();
        Set<VectorClock> committedClocks = new HashSet<>();
        for (ReplicaNode node : replicas) {
            List<StoredEntry> entries = node.localEntries(key);
            if (entries == null) continue;
            for (StoredEntry e : entries) {
                if (e.state() == EntryState.COMMITTED && committedClocks.add(e.version().clock())) {
                    committed.add(e.version());
                }
            }
        }
        for (var d : decisions.entrySet()) {
            if (d.getValue().commit() && key.equals(d.getValue().key()) && committedClocks.add(d.getKey())) {
                // 时钟-only 占位：值通常已存在于某副本；无值时 tombstone 仍保留，删除不会被误判成存活数据
                committed.add(new VersionedValue(null, d.getKey(), d.getValue().tombstone()));
            }
        }
        List<VersionedValue> target = reconcile(committed);
        List<Map<String, Long>> targetClockViews = target.stream()
                .map(v -> v.clock().entries()).toList();
        Map<String, Long> targetContext = mergeAll(target).entries();

        List<ReplicaDiff> diffs = new ArrayList<>();
        List<VersionView> residue = new ArrayList<>();
        boolean consistent = true;

        for (ReplicaNode node : replicas) {
            List<StoredEntry> entries = node.localEntries(key);
            if (entries == null) {
                consistent = false;
                diffs.add(new ReplicaDiff(node.id(), false, DiffKind.NODE_DOWN, List.of(),
                        "节点下线，无法观察"));
                continue;
            }
            Set<VectorClock> localCommitted = new HashSet<>();
            List<VersionView> views = new ArrayList<>();
            List<StoredEntry> dirty = new ArrayList<>();
            for (StoredEntry e : entries) {
                views.add(toView(node.id(), e));
                if (e.state() == EntryState.COMMITTED) localCommitted.add(e.version().clock());
                else dirty.add(e); // PENDING 或 ABORTED
            }

            DiffKind kind;
            String detail;
            if (!dirty.isEmpty()) {
                kind = DiffKind.UNCOMMITTED_RESIDUE;
                detail = describeResidue(dirty);
                for (StoredEntry e : dirty) residue.add(toView(node.id(), e));
            } else if (target.isEmpty()) {
                kind = DiffKind.CONSISTENT;
                detail = entries.isEmpty() ? "无数据，与目标一致（新键）" : "只存在非已提交版本";
            } else if (localCommitted.size() == target.size()
                    && target.stream().allMatch(v -> localCommitted.contains(v.clock()))) {
                kind = DiffKind.CONSISTENT;
                detail = "已提交版本与目标一致";
            } else if (localCommitted.isEmpty()) {
                kind = DiffKind.MISSING_COMMITTED;
                detail = "缺少全部 " + target.size() + " 个已提交版本（提交完成后暂时落后）";
            } else {
                kind = DiffKind.COMMITTED_LAGGING;
                long missing = target.stream().filter(v -> !localCommitted.contains(v.clock())).count();
                detail = "已提交版本落后：还缺 " + missing + " 个已提交版本（正常的暂时落后，不是未提交残留）";
            }
            if (kind != DiffKind.CONSISTENT) consistent = false;
            diffs.add(new ReplicaDiff(node.id(), true, kind, views, detail));
        }

        String summary = consistent
                ? "key=" + key + " 各副本一致"
                : "key=" + key + " 存在不一致（见各副本 kind/detail）";
        return new KeyReport(key, consistent, targetClockViews, targetContext,
                List.copyOf(diffs), List.copyOf(residue), summary);
    }

    /** 只读巡检整库：汇总所有在线副本持有的键。 */
    public ClusterReport inspectAll() {
        Set<String> keys = collectAllKeys();
        List<KeyReport> reports = new ArrayList<>();
        List<String> inconsistent = new ArrayList<>();
        for (String key : keys) {
            KeyReport report = inspectKey(key);
            reports.add(report);
            if (!report.consistent()) inconsistent.add(key);
        }
        String summary = "巡检键数=" + keys.size() + "，不一致键数=" + inconsistent.size()
                + (inconsistent.isEmpty() ? "" : "：" + inconsistent);
        return new ClusterReport(keys.size(), inconsistent.isEmpty(),
                List.copyOf(inconsistent), List.copyOf(reports), summary);
    }

    // ---- 按需收敛（可反复执行、幂等；绝不提升未提交写） ----

    /** 收敛单个键：传播已提交版本、按决定清理残留；对未知 PENDING 先补一条持久化 ABORT 决定。 */
    public RepairResult repair(String key) {
        List<ReplicaNode> replicas = registry.replicasFor(key);

        // 1) 收集在线副本的全部版本
        List<VersionedValue> committed = new ArrayList<>();
        Set<VectorClock> pendingClocks = new HashSet<>();
        Set<VectorClock> abortedClocks = new HashSet<>();
        int reached = 0;
        for (ReplicaNode node : replicas) {
            List<StoredEntry> entries = node.localEntries(key);
            if (entries == null) continue;
            reached++;
            for (StoredEntry e : entries) {
                switch (e.state()) {
                    case COMMITTED -> committed.add(e.version());
                    case PENDING -> pendingClocks.add(e.version().clock());
                    case ABORTED -> abortedClocks.add(e.version().clock());
                }
            }
        }

        // 2) 决定每个时钟的归宿：已知决定优先；未知的 PENDING 永远按放弃处理（凑够确认的写必然已有 COMMIT 决定）
        List<DecisionOutcome> decisionOutcomes = new ArrayList<>();
        Set<VectorClock> decided = new HashSet<>();
        List<VectorClock> toAbort = new ArrayList<>();
        for (VectorClock c : pendingClocks) {
            Decision d = decisions.get(c);
            if (d != null && d.commit()) {
                if (decided.add(c)) {
                    decisionOutcomes.add(new DecisionOutcome(c.entries(), "COMMITTED(已有提交决定)"));
                }
                continue;
            }
            if (d == null) {
                recordDecision(false, false, key, c); // 持久化放弃决定，重启后依旧放弃
            }
            if (decided.add(c)) {
                toAbort.add(c);
                decisionOutcomes.add(new DecisionOutcome(c.entries(), "ABORTED(未凑够确认，收敛放弃)"));
            }
        }
        for (VectorClock c : abortedClocks) {
            if (decided.add(c)) {
                toAbort.add(c);
                decisionOutcomes.add(new DecisionOutcome(c.entries(), "ABORTED(放弃决定)"));
            }
        }

        // 3) 目标已提交版本集：归并已提交版本（去掉被支配旧版本，保留并发兄弟）
        List<VersionedValue> target = reconcile(committed);

        // 4) 对每个在线副本做幂等修复：补齐已提交版本 + 清理残留；不碰下线副本
        int repaired = 0, propagated = 0, purged = 0;
        String repairOp = "converge:" + UUID.randomUUID();
        for (ReplicaNode node : replicas) {
            if (!node.isUp()) continue;
            boolean changed = false;
            if (!target.isEmpty() && node.localMergeCommitted(key, target, repairOp)) {
                changed = true;
                propagated++;
            }
            if (!toAbort.isEmpty()) {
                // 先确保放弃决定已下发（幂等），再物理移除残留记录
                for (VectorClock c : toAbort) node.localAbort(key, c);
                if (node.localPurgeAborted(key, toAbort)) {
                    changed = true;
                    purged++;
                }
            }
            if (changed) repaired++;
        }

        boolean changedAfter = propagated > 0 || purged > 0;
        String summary = "key=" + key + " 在线副本=" + reached + "/" + replicas.size()
                + "，补齐已提交版本的副本=" + propagated + "，清理未提交残留的副本=" + purged
                + (reached < replicas.size() ? "；存在下线副本，其上线后再次收敛即可" : "");
        return new RepairResult(key, changedAfter, reached, repaired, propagated, purged,
                decisionOutcomes, summary);
    }

    /** 收敛整库：对所有在线副本持有的键逐个幂等收敛。 */
    public List<RepairResult> repairAll() {
        List<RepairResult> results = new ArrayList<>();
        for (String key : collectAllKeys()) {
            results.add(repair(key));
        }
        return results;
    }

    private Set<String> collectAllKeys() {
        Set<String> keys = new TreeSet<>();
        for (ReplicaNode node : registry.all().values()) {
            Set<String> local = node.localKeys();
            if (local != null) keys.addAll(local);
        }
        for (Decision d : decisions.values()) keys.add(d.key());
        return keys;
    }

    private static VersionView toView(String nodeId, StoredEntry e) {
        return new VersionView(nodeId, e.state(), e.version().value(),
                e.version().tombstone(), e.version().clock().entries());
    }

    private static String describeResidue(List<StoredEntry> dirty) {
        List<String> parts = new ArrayList<>();
        for (StoredEntry e : dirty) {
            parts.add(e.state() + " clock=" + e.version().clock());
        }
        return "存在未提交干净的残留：" + String.join("; ", parts);
    }

    /** 归并版本集合：去掉被其他版本支配（更旧）的版本，保留并发兄弟。 */
    static List<VersionedValue> reconcile(List<VersionedValue> versions) {
        List<VersionedValue> result = new ArrayList<>();
        for (VersionedValue v : versions) {
            boolean dominated = false;
            Iterator<VersionedValue> it = result.iterator();
            while (it.hasNext()) {
                VersionedValue u = it.next();
                switch (VectorClock.compare(u.clock(), v.clock())) {
                    case BEFORE -> it.remove();
                    case EQUAL, AFTER -> dominated = true;
                    case CONCURRENT -> { }
                }
            }
            if (!dominated) result.add(v);
        }
        return result;
    }

    private static VectorClock mergeAll(List<VersionedValue> versions) {
        VectorClock ctx = new VectorClock();
        for (VersionedValue v : versions) {
            ctx = VectorClock.merge(ctx, v.clock());
        }
        return ctx;
    }
}
