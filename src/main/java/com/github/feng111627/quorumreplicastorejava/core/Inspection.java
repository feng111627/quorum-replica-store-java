package com.github.feng111627.quorumreplicastorejava.core;

import java.util.List;
import java.util.Map;

/**
 * 只读一致性巡检与按需收敛的结果类型（同时也是 REST 返回的 JSON 视图，时钟展开为普通 Map）。
 *
 * 巡检对每个 key 收集全部副本（含下线副本）的版本与提交状态，
 * 区分「已提交后暂时落后」与「未提交干净的残留」；巡检不改动任何数据。
 */
public final class Inspection {
    private Inspection() {}

    /** 单个副本相对某个 key 的差异分类。 */
    public enum DiffKind {
        CONSISTENT,           // 与已提交目标一致
        MISSING_COMMITTED,    // 已提交后暂时落后：缺少全部已提交版本（新值尚未传播到）
        COMMITTED_LAGGING,    // 已提交后暂时落后：本地已提交版本比目标旧
        UNCOMMITTED_RESIDUE,  // 未提交干净的残留：存在 PENDING 或已放弃决定的版本
        NODE_DOWN             // 副本下线，无法观察
    }

    /** 某个版本在某副本上的观察结果。 */
    public record VersionView(String nodeId, EntryState state,
                              String value, boolean tombstone,
                              Map<String, Long> clock) {}

    /** 单个副本对某个 key 的巡检行。 */
    public record ReplicaDiff(String nodeId, boolean reachable, DiffKind kind,
                              List<VersionView> versions, String detail) {}

    /** 单个 key 的巡检结果。 */
    public record KeyReport(String key, boolean consistent,
                            List<Map<String, Long>> committedClocks,
                            Map<String, Long> committedContext,
                            List<ReplicaDiff> replicas,
                            List<VersionView> residueVersions,
                            String summary) {}

    /** 整库巡检结果。 */
    public record ClusterReport(int keyCount, boolean consistent,
                                List<String> inconsistentKeys,
                                List<KeyReport> keys, String summary) {}

    /** 收敛时对某个版本时钟作出的归宿说明。 */
    public record DecisionOutcome(Map<String, Long> clock, String outcome) {}

    /** 按需收敛结果。 */
    public record RepairResult(String key, boolean changed,
                               int replicasReached, int replicasRepaired,
                               int committedPropagated, int residuePurged,
                               List<DecisionOutcome> decisions,
                               String summary) {}
}
