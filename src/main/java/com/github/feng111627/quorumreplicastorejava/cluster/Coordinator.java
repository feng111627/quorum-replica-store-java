package com.github.feng111627.quorumreplicastorejava.cluster;

import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.ValueView;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/** 协调器：把客户端读写分发到分片副本，按 W/R 法定人数判定成败，并做读修复。 */
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

        int accepted = 0, stale = 0;
        for (ReplicaNode node : registry.replicasFor(key)) {
            ReplicaNode.WriteAck ack = node.localPut(opId, key, value, context, newClock, tombstone);
            if (ack.accepted()) accepted++;
            else if (ack.stale()) stale++;
        }
        if (accepted >= writeQuorum) {
            return WriteResult.ok(newClock);
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
            List<VersionedValue> local = node.localGet(key);
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

        // 读修复：把归并结果回写到在线副本，避免已提交数据在副本切换后回退
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
