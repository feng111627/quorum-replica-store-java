package com.github.feng111627.quorumreplicastorejava.shard;

import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;

import java.util.ArrayList;
import java.util.List;

/**
 * 分片布局：按键哈希把键空间切成固定数量的分片，每个分片映射到一组副本节点。
 * 键 → (hash mod shardCount) → 该分片的副本列表。
 */
public class ShardLayout {

    private final int shardCount;
    private final List<List<ReplicaNode>> shardReplicas;

    public ShardLayout(int shardCount, List<List<ReplicaNode>> shardReplicas) {
        if (shardCount <= 0 || shardReplicas.size() != shardCount) {
            throw new IllegalArgumentException("分片数与副本组数量不一致");
        }
        this.shardCount = shardCount;
        this.shardReplicas = List.copyOf(shardReplicas);
    }

    public int shardFor(String key) {
        return Math.floorMod(key.hashCode(), shardCount);
    }

    public List<ReplicaNode> replicasFor(String key) {
        return shardReplicas.get(shardFor(key));
    }

    public List<ReplicaNode> allNodes() {
        List<ReplicaNode> all = new ArrayList<>();
        shardReplicas.forEach(all::addAll);
        return all;
    }
}
