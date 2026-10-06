package com.github.feng111627.quorumreplicastorejava.api;

import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/** 管理接口：手动上下线节点，模拟宕机与分区。 */
@RestController
@RequestMapping("/admin")
public class AdminController {
    private final NodeRegistry registry;

    public AdminController(NodeRegistry registry) {
        this.registry = registry;
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

    @GetMapping("/nodes/{nodeId}/keys/{key}")
    public ResponseEntity<?> localGet(@PathVariable String nodeId, @PathVariable String key) {
        ReplicaNode node = registry.node(nodeId);
        var values = node.localGet(key);
        if (values == null) return ResponseEntity.status(503).body(Map.of("error", "node is down"));
        return ResponseEntity.ok(values.stream()
                .map(v -> Map.of("value", v.value() == null ? "" : v.value(),
                                 "tombstone", v.tombstone(),
                                 "clock", v.clock().entries()))
                .toList());
    }
}
