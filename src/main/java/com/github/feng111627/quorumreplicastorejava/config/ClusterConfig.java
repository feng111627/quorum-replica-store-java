package com.github.feng111627.quorumreplicastorejava.config;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.cluster.NodeRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

/** 集群装配：本地单进程模拟多副本节点。默认 5 节点、3 分片、每分片 3 副本、W=2/R=2。 */
@Configuration
public class ClusterConfig {

    @Bean
    public NodeRegistry nodeRegistry(
            @Value("${cluster.nodes:node-1,node-2,node-3,node-4,node-5}") List<String> nodes,
            @Value("${cluster.shard-count:3}") int shardCount,
            @Value("${cluster.replicas-per-shard:3}") int replicasPerShard,
            @Value("${cluster.data-dir:./data}") String dataDir) {
        return new NodeRegistry(nodes, shardCount, replicasPerShard, Path.of(dataDir));
    }

    @Bean
    public Coordinator coordinator(
            NodeRegistry registry,
            @Value("${cluster.write-quorum:2}") int writeQuorum,
            @Value("${cluster.read-quorum:2}") int readQuorum,
            @Value("${cluster.data-dir:./data}") String dataDir) {
        return new Coordinator(registry, writeQuorum, readQuorum, Path.of(dataDir));
    }
}
