package com.github.feng111627.quorumreplicastorejava.cluster;

import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;

import java.util.List;
import java.util.Map;

/**
 * 一致性巡检与按需收敛的结果类型。
 * 巡检是只读的：只收集各副本的版本视图做对比，不改动任何数据。
 * 所有类型可直接 JSON 序列化（时钟以 Map 表示），供 REST 接口返回。
 */
public final class Inspection {
    private Inspection() {}

    /** 诊断结论：区分"正常提交后暂时落后"与"未提交干净的残留"。 */
    public enum Diagnosis {
        CONSISTENT,           // 所有在线副本的已提交版本一致，且无未提交残留
        LAGGING,              // 部分副本暂时落后（缺少已提交版本），属正常复制延迟
        UNCOMMITTED_RESIDUE,  // 存在未提交（未凑够法定人数）的残留版本
        MIXED                 // 既有副本落后又有未提交残留
    }

    /** 可 JSON 序列化的版本视图：值 + 向量时钟（Map 表示）+ 是否墓碑。 */
    public record VersionView(String value, Map<String, Long> clock, boolean tombstone) {
        public static VersionView of(VersionedValue v) {
            return new VersionView(v.value(), v.clock().entries(), v.tombstone());
        }
    }

    /** 单个副本上某键的版本视图。up=false 表示节点下线，无法采集。 */
    public record ReplicaSnapshot(String nodeId, boolean up,
                                  List<VersionView> committed,
                                  List<PendingView> pending) {}

    /** 未提交（pending）版本：带着产生它的 opId，便于追踪与清理。 */
    public record PendingView(String opId, VersionView value) {}

    /** 单个键的巡检报告。 */
    public record KeyReport(String key,
                            Diagnosis diagnosis,
                            List<String> laggingNodes,   // 已提交版本落后于归并结果的副本
                            List<String> residueNodes,   // 持有未提交残留的副本
                            List<String> downNodes,      // 下线、未能采集的副本
                            List<ReplicaSnapshot> replicas,
                            String message) {}

    /** 一次巡检（单键或全库）的整体报告。 */
    public record InspectReport(List<KeyReport> keys, String summary) {}

    /** 单个键的收敛结果。 */
    public record KeyConvergence(String key,
                                 List<String> repairedNodes,   // 被补齐已提交版本的副本
                                 List<String> abortedResidue,  // 被清除的未提交残留（nodeId:opId）
                                 Diagnosis after,              // 收敛后再巡检的结论
                                 String message) {}

    /** 一次收敛（单键或全库）的整体报告。 */
    public record ConvergeReport(List<KeyConvergence> keys, String summary) {}
}
