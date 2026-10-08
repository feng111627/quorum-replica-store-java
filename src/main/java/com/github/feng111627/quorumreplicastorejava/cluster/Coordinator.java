package com.github.feng111627.quorumreplicastorejava.cluster;

import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.ConvergeReport;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.Diagnosis;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.InspectReport;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.KeyConvergence;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.KeyReport;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.PendingView;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.ReplicaSnapshot;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.VersionView;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.ValueView;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * 协调器：把客户端读写分发到分片副本，按 W/R 法定人数判定成败，并做读修复。
 *
 * 写入为两阶段：先 prepare（值进副本 pending 区，对读不可见），
 * 凑够 W 个确认才 commit；凑不够则 abort 所有已 prepare 的副本——
 * 失败的写当作从未发生：任何副本都不会把它返回给读，重启后也不会复活。
 *
 * 假设（单进程模拟）：单次写期间协调器自身不宕机、副本上下线状态不变。
 */
public class Coordinator {
    private final NodeRegistry registry;
    private final int writeQuorum;
    private final int readQuorum;

    public Coordinator(NodeRegistry registry, int writeQuorum, int readQuorum) {
        this.registry = registry;
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
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

        List<ReplicaNode> replicas = registry.replicasFor(key);

        // 阶段一：prepare——值只进各副本的 pending 区，对读不可见
        int accepted = 0, stale = 0;
        for (ReplicaNode node : replicas) {
            ReplicaNode.WriteAck ack = node.localPut(opId, key, value, context, newClock, tombstone);
            if (ack.accepted()) accepted++;
            else if (ack.stale()) stale++;
        }

        if (accepted >= writeQuorum) {
            // 阶段二（成功）：提交——pending 版本并入各副本已提交表，对读可见
            for (ReplicaNode node : replicas) {
                node.commitOp(opId, key);
            }
            return WriteResult.ok(newClock);
        }

        // 阶段二（失败）：撤销——所有已 prepare 的副本清理 pending，当作从未发生
        for (ReplicaNode node : replicas) {
            node.abortOp(opId, key);
        }
        if (stale > 0) {
            return WriteResult.staleClock();
        }
        return WriteResult.quorumNotMet(accepted, writeQuorum);
    }

    public ReadResult get(String key) {
        List<ReplicaNode> replicas = registry.replicasFor(key);
        List<VersionedValue> all = new ArrayList<>();
        int responded = 0;
        List<ReplicaNode> upNodes = new ArrayList<>();
        for (ReplicaNode node : replicas) {
            List<VersionedValue> local = node.localGet(key); // 只返回已提交版本
            if (local == null) continue; // 节点下线，无响应
            responded++;
            upNodes.add(node);
            all.addAll(local);
        }
        if (responded < readQuorum) {
            return ReadResult.quorumNotMet(responded, readQuorum);
        }

        List<VersionedValue> reconciled = reconcile(all);
        VectorClock context = mergeAll(all);

        // 读修复：把归并后的已提交结果回写到在线副本，避免已提交数据在副本切换后回退
        if (!reconciled.isEmpty()) {
            String repairOp = "repair:" + UUID.randomUUID();
            for (ReplicaNode node : upNodes) {
                node.localMerge(key, reconciled, repairOp);
            }
        }

        List<VersionedValue> live = reconciled.stream().filter(v -> !v.tombstone()).toList();
        if (live.isEmpty()) {
            return ReadResult.notFound(context);
        }
        if (live.size() > 1) {
            return ReadResult.conflict(live.stream().map(ValueView::of).toList(), context);
        }
        return ReadResult.ok(ValueView.of(live.get(0)), context);
    }

    // ---------------- 只读一致性巡检 ----------------

    /** 巡检单个键：收集各副本版本视图并诊断，不改动任何数据。 */
    public InspectReport inspect(String key) {
        return new InspectReport(List.of(inspectKey(key)), "inspected 1 key");
    }

    /** 巡检整个库：对所有在线副本上出现过的键逐一诊断，不改动任何数据。 */
    public InspectReport inspectAll() {
        Set<String> keys = allKeys();
        List<KeyReport> reports = new ArrayList<>();
        for (String key : keys) {
            reports.add(inspectKey(key));
        }
        long inconsistent = reports.stream().filter(r -> r.diagnosis() != Diagnosis.CONSISTENT).count();
        return new InspectReport(List.copyOf(reports),
                "inspected " + reports.size() + " keys, " + inconsistent + " inconsistent");
    }

    private KeyReport inspectKey(String key) {
        List<ReplicaNode> replicas = registry.replicasFor(key);
        List<ReplicaSnapshot> snapshots = new ArrayList<>();
        List<ReplicaNode.Snapshot> raws = new ArrayList<>();
        List<VersionedValue> allCommitted = new ArrayList<>();
        List<String> downNodes = new ArrayList<>();

        for (ReplicaNode node : replicas) {
            ReplicaNode.Snapshot snap = node.localInspect(key);
            if (snap == null) {
                downNodes.add(node.id());
                snapshots.add(new ReplicaSnapshot(node.id(), false, List.of(), List.of()));
                raws.add(null);
                continue;
            }
            List<VersionView> committed = snap.committed().stream().map(VersionView::of).toList();
            List<PendingView> pending = snap.pending().stream()
                    .map(p -> new PendingView(p.opId(), VersionView.of(p.value())))
                    .toList();
            snapshots.add(new ReplicaSnapshot(node.id(), true, committed, pending));
            raws.add(snap);
            allCommitted.addAll(snap.committed());
        }

        // 归并后的已提交目标状态：判断"谁落后"的基准
        List<VersionedValue> target = reconcile(allCommitted);
        Set<VersionedValue> targetSet = new LinkedHashSet<>(target);

        List<String> lagging = new ArrayList<>();
        List<String> residue = new ArrayList<>();
        for (int i = 0; i < replicas.size(); i++) {
            ReplicaNode.Snapshot raw = raws.get(i);
            if (raw == null) continue;
            if (!new LinkedHashSet<>(raw.committed()).equals(targetSet)) {
                lagging.add(replicas.get(i).id());
            }
            if (!raw.pending().isEmpty()) {
                residue.add(replicas.get(i).id());
            }
        }

        Diagnosis diagnosis;
        if (!residue.isEmpty() && !lagging.isEmpty()) diagnosis = Diagnosis.MIXED;
        else if (!residue.isEmpty()) diagnosis = Diagnosis.UNCOMMITTED_RESIDUE;
        else if (!lagging.isEmpty()) diagnosis = Diagnosis.LAGGING;
        else diagnosis = Diagnosis.CONSISTENT;

        String message = switch (diagnosis) {
            case CONSISTENT -> "all reachable replicas hold identical committed versions, no uncommitted residue";
            case LAGGING -> "replicas " + lagging + " are missing committed versions (normal replication lag, convergent)";
            case UNCOMMITTED_RESIDUE -> "replicas " + residue + " hold uncommitted residue from writes that never reached quorum";
            case MIXED -> "lagging replicas " + lagging + " plus uncommitted residue on " + residue;
        };
        return new KeyReport(key, diagnosis,
                List.copyOf(lagging), List.copyOf(residue), List.copyOf(downNodes),
                List.copyOf(snapshots), message);
    }

    // ---------------- 按需收敛 ----------------

    /** 收敛单个键：把已提交状态修到一致，并清除未提交残留。幂等，可反复执行。 */
    public ConvergeReport converge(String key) {
        return new ConvergeReport(List.of(convergeKey(key)), "converged 1 key");
    }

    /** 收敛整个库。幂等，可反复执行。 */
    public ConvergeReport convergeAll() {
        Set<String> keys = allKeys();
        List<KeyConvergence> results = new ArrayList<>();
        for (String key : keys) {
            results.add(convergeKey(key));
        }
        return new ConvergeReport(List.copyOf(results), "converged " + results.size() + " keys");
    }

    /**
     * 收敛单个键：
     *   1. 归并各在线副本的已提交版本为目标状态，回写到所有在线副本（补齐落后者）；
     *   2. 清除所有未提交残留——绝不把未凑够法定人数的写提升为可读数据。
     *      （若某残留对应的写其实已在别处提交，已提交副本重上线后会经读修复/收敛再次扩散，
     *        清除本地残留不影响那条已提交写的持久性。）
     */
    private KeyConvergence convergeKey(String key) {
        List<ReplicaNode> replicas = registry.replicasFor(key);
        List<ReplicaNode> upNodes = new ArrayList<>();
        List<VersionedValue> allCommitted = new ArrayList<>();
        for (ReplicaNode node : replicas) {
            ReplicaNode.Snapshot snap = node.localInspect(key);
            if (snap == null) continue;
            upNodes.add(node);
            allCommitted.addAll(snap.committed());
        }

        // 1. 已提交目标状态回写（幂等，副本无变化时不落盘）
        List<VersionedValue> target = reconcile(allCommitted);
        List<String> repaired = new ArrayList<>();
        if (!target.isEmpty()) {
            String convergeOp = "converge:" + UUID.randomUUID();
            for (ReplicaNode node : upNodes) {
                if (node.localMerge(key, target, convergeOp)) {
                    repaired.add(node.id());
                }
            }
        }

        // 2. 清除未提交残留（abort 是幂等的，且写 WAL，重启后不会复活）
        List<String> aborted = new ArrayList<>();
        for (ReplicaNode node : upNodes) {
            ReplicaNode.Snapshot snap = node.localInspect(key);
            if (snap == null) continue;
            for (ReplicaNode.PendingEntry p : snap.pending()) {
                if (node.abortOp(p.opId(), key)) {
                    aborted.add(node.id() + ":" + p.opId());
                }
            }
        }

        Diagnosis after = inspectKey(key).diagnosis();
        String message = aborted.isEmpty() && repaired.isEmpty()
                ? "already converged, no action taken"
                : "repaired " + repaired + ", aborted uncommitted residue " + aborted;
        return new KeyConvergence(key, List.copyOf(repaired), List.copyOf(aborted), after, message);
    }

    /** 所有在线副本上出现过（已提交或未提交）的键的并集。 */
    private Set<String> allKeys() {
        Set<String> keys = new TreeSet<>();
        for (ReplicaNode node : registry.all().values()) {
            Set<String> local = node.localKeys();
            if (local != null) keys.addAll(local);
        }
        return keys;
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
