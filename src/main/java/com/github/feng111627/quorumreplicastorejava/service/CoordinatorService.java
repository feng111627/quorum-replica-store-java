package com.github.feng111627.quorumreplicastorejava.service;

import com.github.feng111627.quorumreplicastorejava.model.OpStatus;
import com.github.feng111627.quorumreplicastorejava.model.ReadResult;
import com.github.feng111627.quorumreplicastorejava.model.Version;
import com.github.feng111627.quorumreplicastorejava.model.VersionedValue;
import com.github.feng111627.quorumreplicastorejava.model.WriteResult;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import com.github.feng111627.quorumreplicastorejava.shard.ShardLayout;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * 协调器：把读写路由到键所属分片的副本，按 W/R 法定人数收集确认。
 * 一致性规则：
 *  - 写：baseVersion 必须等于当前已提交的最大版本（新键传 null），否则 CONFLICT；
 *    凑齐 W 个副本确认才返回 OK；不足 W 时回滚已确认副本（失败写不留痕迹），
 *    返回 WRITE_QUORUM_NOT_MET。
 *  - 读：凑齐 R 个副本应答才返回，取其中最大版本；不足 R 返回 READ_QUORUM_NOT_MET，
 *    绝不返回部分结果或悄悄给旧值。
 *  - 读修复：把读到的最大版本回写给落后副本（节点侧保证过期数据不覆盖新数据）。
 */
public class CoordinatorService {

    private final ShardLayout layout;
    private final int writeQuorum;
    private final int readQuorum;
    private final String writerId = "coordinator";

    public CoordinatorService(ShardLayout layout, int writeQuorum, int readQuorum) {
        this.layout = layout;
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
    }

    /**
     * 写入。baseVersion 为客户端读到的版本（null 表示期望创建新键）；
     * 与已提交版本不一致时返回 CONFLICT。整个写路径串行化以保证冲突检测正确。
     */
    public synchronized WriteResult write(String key, String value, Version baseVersion, String opId) {
        List<ReplicaNode> replicas = layout.replicasFor(key);

        // 1. 从在线副本收集当前版本
        List<VersionedValue> current = new ArrayList<>();
        for (ReplicaNode node : replicas) {
            if (node.isUp()) {
                node.get(key).ifPresent(current::add);
            }
        }
        Optional<VersionedValue> latest = current.stream()
                .max(Comparator.comparing(VersionedValue::version));

        // 2. 幂等：同一 opId 已提交过，直接返回原结果（重试/重放安全）
        if (latest.isPresent() && latest.get().opId().equals(opId)) {
            return WriteResult.ok(latest.get().version());
        }

        // 3. 冲突检测：baseVersion 必须匹配当前最大已提交版本
        if (latest.isPresent()) {
            if (baseVersion == null || !baseVersion.equals(latest.get().version())) {
                return WriteResult.fail(OpStatus.CONFLICT,
                        "键冲突：期望版本 " + baseVersion + "，当前已提交版本 " + latest.get().version());
            }
        } else if (baseVersion != null) {
            return WriteResult.fail(OpStatus.CONFLICT,
                    "键冲突：键不存在但客户端携带了基线版本 " + baseVersion);
        }

        // 4. 生成新版本并广播到所有在线副本（先记录各副本写入前的状态，便于失败回滚）
        long nextSeq = latest.map(v -> v.version().sequence() + 1).orElse(1L);
        VersionedValue newValue = new VersionedValue(value, new Version(nextSeq, writerId), opId);
        List<ReplicaNode> acked = new ArrayList<>();
        List<VersionedValue> previous = new ArrayList<>();
        for (ReplicaNode node : replicas) {
            if (!node.isUp()) {
                continue;
            }
            VersionedValue prev = node.get(key).orElse(null);
            if (node.put(key, newValue)) {
                acked.add(node);
                previous.add(prev);
            }
        }

        // 5. 法定人数判定：不足时回滚已确认副本，失败的写不留痕迹
        if (acked.size() >= writeQuorum) {
            return WriteResult.ok(newValue.version());
        }
        for (int i = 0; i < acked.size(); i++) {
            acked.get(i).rollback(key, opId, previous.get(i));
        }
        return WriteResult.fail(OpStatus.WRITE_QUORUM_NOT_MET,
                "写法定人数不足：需要 " + writeQuorum + "，实际确认 " + acked.size()
                        + "，已回滚 " + acked.size() + " 个副本");
    }

    /** 读取。凑不齐读法定人数时返回 READ_QUORUM_NOT_MET，绝不返回部分结果。 */
    public synchronized ReadResult read(String key) {
        List<ReplicaNode> replicas = layout.replicasFor(key);
        List<VersionedValue> responses = new ArrayList<>();
        List<ReplicaNode> responders = new ArrayList<>();
        for (ReplicaNode node : replicas) {
            if (node.isUp()) {
                Optional<VersionedValue> v = node.get(key);
                responders.add(node);
                v.ifPresent(responses::add);
            }
        }
        if (responders.size() < readQuorum) {
            return ReadResult.fail(OpStatus.READ_QUORUM_NOT_MET,
                    "读法定人数不足：需要 " + readQuorum + "，在线副本 " + responders.size());
        }
        if (responses.isEmpty()) {
            return ReadResult.fail(OpStatus.KEY_NOT_FOUND, "键不存在: " + key);
        }
        VersionedValue latest = responses.stream()
                .max(Comparator.comparing(VersionedValue::version)).orElseThrow();
        readRepair(key, replicas, latest);
        return ReadResult.ok(latest);
    }

    /** 读修复：把最大版本回写给落后副本（节点侧版本检查保证不会回退）。 */
    private void readRepair(String key, List<ReplicaNode> replicas, VersionedValue latest) {
        for (ReplicaNode node : replicas) {
            if (!node.isUp()) {
                continue;
            }
            Optional<VersionedValue> local = node.get(key);
            if (local.isEmpty() || local.get().version().compareTo(latest.version()) < 0) {
                node.put(key, latest);
            }
        }
    }
}
