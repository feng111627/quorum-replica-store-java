package com.github.feng111627.quorumreplicastorejava.config;

import com.github.feng111627.quorumreplicastorejava.node.ReplicaNode;
import com.github.feng111627.quorumreplicastorejava.service.CoordinatorService;
import com.github.feng111627.quorumreplicastorejava.shard.ShardLayout;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 组装本地模拟集群：默认 3 个分片，每分片 3 副本，W=2，R=2。
 * 每个节点在 data-dir 下有独立的 WAL 文件模拟本地落盘。
 */
@Configuration
public class StoreConfig {

    @Bean
    public ShardLayout shardLayout(
            @Value("${store.data-dir:./data}") String dataDir,
            @Value("${store.shard-count:3}") int shardCount,
            @Value("${store.replicas-per-shard:3}") int replicasPerShard) {
        List<List<ReplicaNode>> groups = new ArrayList<>();
        for (int s = 0; s < shardCount; s++) {
            List<ReplicaNode> group = new ArrayList<>();
            for (int r = 0; r < replicasPerShard; r++) {
                String nodeId = "shard" + s + "-node" + r;
                group.add(new ReplicaNode(nodeId, Path.of(dataDir)));
            }
            groups.add(group);
        }
        return new ShardLayout(shardCount, groups);
    }

    @Bean
    public CoordinatorService coordinatorService(
            ShardLayout layout,
            @Value("${store.write-quorum:2}") int writeQuorum,
            @Value("${store.read-quorum:2}") int readQuorum) {
        return new CoordinatorService(layout, writeQuorum, readQuorum);
    }

    @Bean
    public List<ReplicaNode> nodes(ShardLayout layout) {
        return layout.allNodes();
    }
}
