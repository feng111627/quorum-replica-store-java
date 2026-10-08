package com.github.feng111627.quorumreplicastorejava.api;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.ConvergeReport;
import com.github.feng111627.quorumreplicastorejava.cluster.Inspection.InspectReport;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/** 管理接口：手动上下线节点（模拟宕机/分区）、只读一致性巡检、按需收敛。 */
@RestController
@RequestMapping("/admin")
public class AdminController {
    private final NodeRegistry registry;
    private final Coordinator coordinator;

    public AdminController(NodeRegistry registry, Coordinator coordinator) {
        this.registry = registry;
        this.coordinator = coordinator;
    }

    public record NodeState(String nodeId, boolean up) {}

    @PostMapping("/nodes/{nodeId}/down")
    public NodeState down(@PathVariable String nodeId) {
        registry.node(nodeId).down();
        return new NodeState(nodeId, false);
    }

    @PostMapping("/nodes/{nodeId}/up")
    public NodeState up(@PathVariable String nodeId) {
        registry.node(nodeId).up();
        return new NodeState(nodeId, true);
    }

    @GetMapping("/nodes")
    public Map<String, Boolean> nodes() {
        Map<String, Boolean> states = new LinkedHashMap<>();
        registry.all().forEach((id, n) -> states.put(id, n.isUp()));
        return states;
    }

    /** 查看某节点本地版本：committed 为已提交（可读），pending 为未提交残留（对读不可见）。 */
    @GetMapping("/nodes/{nodeId}/keys/{key}")
    public ResponseEntity<?> localGet(@PathVariable String nodeId, @PathVariable String key) {
        ReplicaNode node = registry.node(nodeId);
        ReplicaNode.Snapshot snap = node.localInspect(key);
        if (snap == null) return ResponseEntity.status(503).body(Map.of("error", "node is down"));
        return ResponseEntity.ok(Map.of(
                "committed", snap.committed().stream()
                        .map(v -> Map.of("value", v.value() == null ? "" : v.value(),
                                         "tombstone", v.tombstone(),
                                         "clock", v.clock().entries()))
                        .toList(),
                "pending", snap.pending().stream()
                        .map(p -> Map.of("opId", p.opId(),
                                         "value", p.value().value() == null ? "" : p.value().value(),
                                         "tombstone", p.value().tombstone(),
                                         "clock", p.value().clock().entries()))
                        .toList()));
    }

    /**
     * 只读一致性巡检：?key=xxx 巡检单个键，不带参数巡检整个库。
     * 对比各副本的已提交版本，区分"正常提交后暂时落后"与"未提交干净的残留"，不改动任何数据。
     */
    @GetMapping("/inspect")
    public InspectReport inspect(@RequestParam(required = false) String key) {
        return key != null ? coordinator.inspect(key) : coordinator.inspectAll();
    }

    /**
     * 按需收敛：?key=xxx 收敛单个键，不带参数收敛整个库。
     * 把已提交状态修到一致并清除未提交残留；幂等，可反复调用；
     * 绝不会把未凑够法定人数的写提升为可读数据。
     */
    @PostMapping("/converge")
    public ConvergeReport converge(@RequestParam(required = false) String key) {
        return key != null ? coordinator.converge(key) : coordinator.convergeAll();
    }
}
