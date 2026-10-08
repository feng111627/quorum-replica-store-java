package com.github.feng111627.quorumreplicastorejava.api;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.ClusterReport;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.KeyReport;
import com.github.feng111627.quorumreplicastorejava.core.Inspection.RepairResult;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import com.github.feng111627.quorumreplicastorejava.node.StoredEntry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 管理接口：手动上下线节点（模拟宕机/分区）、查看副本本地版本、
 * 只读一致性巡检（inspect）与按需幂等收敛（repair）。
 */
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

    /** 查看某节点本地版本（含提交状态 PENDING/COMMITTED/ABORTED）。 */
    @GetMapping("/nodes/{nodeId}/keys/{key}")
    public ResponseEntity<?> localGet(@PathVariable String nodeId, @PathVariable String key) {
        ReplicaNode node = registry.node(nodeId);
        List<StoredEntry> entries = node.localEntries(key);
        if (entries == null) return ResponseEntity.status(503).body(Map.of("error", "node is down"));
        return ResponseEntity.ok(entries.stream()
                .map(e -> Map.of("value", e.version().value() == null ? "" : e.version().value(),
                                 "tombstone", e.version().tombstone(),
                                 "state", e.state().name(),
                                 "clock", e.version().clock().entries()))
                .toList());
    }

    // ---- 只读一致性巡检 ----

    /** 巡检单个键：不改动任何数据。 */
    @GetMapping("/inspect/keys/{key}")
    public KeyReport inspectKey(@PathVariable String key) {
        return coordinator.inspectKey(key);
    }

    /** 巡检整库：不改动任何数据。 */
    @GetMapping("/inspect")
    public ClusterReport inspectAll() {
        return coordinator.inspectAll();
    }

    // ---- 按需收敛（幂等，可重复触发） ----

    /** 收敛单个键。 */
    @PostMapping("/repair/keys/{key}")
    public RepairResult repairKey(@PathVariable String key) {
        return coordinator.repair(key);
    }

    /** 收敛整库。 */
    @PostMapping("/repair")
    public List<RepairResult> repairAll() {
        return coordinator.repairAll();
    }
}
