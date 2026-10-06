package com.github.feng111627.quorumreplicastorejava.cluster;

import com.github.feng111627.quorumreplicastorejava.core.ShardRouter;
import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 集群注册表：持有所有节点与分片路由。 */
public class NodeRegistry {
    private final Map<String, ReplicaNode> nodes = new LinkedHashMap<>();
    private final ShardRouter router;

    public NodeRegistry(List<String> nodeIds, int shardCount, int replicasPerShard, Path dataRoot) {
        this.router = new ShardRouter(shardCount, nodeIds, replicasPerShard);
        for (String id : nodeIds) {
            nodes.put(id, new ReplicaNode(id, dataRoot));
        }
    }

    public ReplicaNode node(String nodeId) {
        ReplicaNode n = nodes.get(nodeId);
        if (n == null) throw new IllegalArgumentException("unknown node: " + nodeId);
        return n;
    }

    public List<ReplicaNode> replicasFor(String key) {
        return router.replicasOf(key).stream().map(this::node).toList();
    }

    public ShardRouter router() {
        return router;
    }

    public Map<String, ReplicaNode> all() {
        return Map.copyOf(nodes);
    }
}
