package com.github.feng111627.quorumreplicastorejava.web;

import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 管理接口：手动上/下线节点、触发重启恢复，用于模拟宕机与分区。
 */
@RestController
@RequestMapping("/admin/nodes")
public class AdminController {

    private final List<ReplicaNode> nodes;

    public AdminController(List<ReplicaNode> nodes) {
        this.nodes = nodes;
    }

    @GetMapping
    public Map<String, Boolean> list() {
        Map<String, Boolean> status = new java.util.TreeMap<>();
        nodes.forEach(n -> status.put(n.id(), n.isUp()));
        return status;
    }

    @PostMapping("/{nodeId}/down")
    public ResponseEntity<?> down(@PathVariable String nodeId) {
        ReplicaNode node = find(nodeId);
        if (node == null) {
            return ResponseEntity.status(404).body(Map.of("message", "节点不存在: " + nodeId));
        }
        node.setUp(false);
        return ResponseEntity.ok(Map.of("node", nodeId, "up", false));
    }

    @PostMapping("/{nodeId}/up")
    public ResponseEntity<?> up(@PathVariable String nodeId) {
        ReplicaNode node = find(nodeId);
        if (node == null) {
            return ResponseEntity.status(404).body(Map.of("message", "节点不存在: " + nodeId));
        }
        node.setUp(true);
        return ResponseEntity.ok(Map.of("node", nodeId, "up", true));
    }

    /** 模拟崩溃重启：从 WAL 恢复到崩溃前已提交状态。 */
    @PostMapping("/{nodeId}/restart")
    public ResponseEntity<?> restart(@PathVariable String nodeId) {
        ReplicaNode node = find(nodeId);
        if (node == null) {
            return ResponseEntity.status(404).body(Map.of("message", "节点不存在: " + nodeId));
        }
        node.recover();
        node.setUp(true);
        return ResponseEntity.ok(Map.of("node", nodeId, "up", true, "recovered", true));
    }

    private ReplicaNode find(String nodeId) {
        return nodes.stream().filter(n -> n.id().equals(nodeId)).findFirst().orElse(null);
    }
}
