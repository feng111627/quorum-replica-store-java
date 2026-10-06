package com.github.feng111627.quorumreplicastorejava.core;

import java.util.ArrayList;
import java.util.List;

/**
 * 按键范围分片：把 key 的哈希空间均匀切成 shardCount 段，
 * 每个分片的副本放在节点环上连续 replicasPerShard 个节点。
 */
public class ShardRouter {
    private final int shardCount;
    private final List<String> nodeIds;
    private final int replicasPerShard;

    public ShardRouter(int shardCount, List<String> nodeIds, int replicasPerShard) {
        if (shardCount <= 0) throw new IllegalArgumentException("shardCount must be > 0");
        if (nodeIds == null || nodeIds.isEmpty()) throw new IllegalArgumentException("nodeIds empty");
        if (replicasPerShard <= 0 || replicasPerShard > nodeIds.size())
            throw new IllegalArgumentException("replicasPerShard must be in [1, nodeCount]");
        this.shardCount = shardCount;
        this.nodeIds = List.copyOf(nodeIds);
        this.replicasPerShard = replicasPerShard;
    }

    /** 键范围分片：按 key 哈希落到 [0, shardCount) 的区间。 */
    public int shardOf(String key) {
        long h = Integer.toUnsignedLong(key.hashCode());
        return (int) (h % shardCount);
    }

    /** 该 key 所在分片的副本节点列表（节点环上连续取 N 个）。 */
    public List<String> replicasOf(String key) {
        int shard = shardOf(key);
        List<String> result = new ArrayList<>(replicasPerShard);
        for (int i = 0; i < replicasPerShard; i++) {
            result.add(nodeIds.get((shard + i) % nodeIds.size()));
        }
        return result;
    }

    public int shardCount() {
        return shardCount;
    }
}
