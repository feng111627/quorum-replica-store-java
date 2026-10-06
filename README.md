# Quorum Replica Store

本地可运行的分布式键值存储模拟：单进程内运行多个副本节点，每个节点用
**内存表 + 本地 WAL 落盘** 模拟独立机器的存储，支持分片、法定人数读写、
并发写冲突检测、节点故障与重启恢复。

## 运行环境

- JDK 21+（开发验证使用 OpenJDK 25）
- Maven 3.9+

## 架构

```
客户端
  │  HTTP
  ▼
KvController / AdminController          (api)
  │
  ▼
Coordinator  ── 读写分发、W/R 法定人数判定、版本归并、读修复   (cluster)
  │
  ▼
NodeRegistry ── ShardRouter（键范围分片 + 节点环副本放置）     (cluster/core)
  │
  ▼
ReplicaNode × N ── 内存 siblings 表 + Wal（追加落盘 + fsync）  (node)
```

默认拓扑（可用 `application.properties` 或启动参数覆盖）：

| 配置 | 默认值 | 说明 |
|---|---|---|
| `cluster.nodes` | node-1..node-5 | 5 个模拟节点 |
| `cluster.shard-count` | 3 | 键空间按哈希切成 3 段 |
| `cluster.replicas-per-shard` | 3 | 每分片 3 副本（N=3） |
| `cluster.write-quorum` | 2 | 写法定人数 W=2 |
| `cluster.read-quorum` | 2 | 读法定人数 R=2 |
| `cluster.data-dir` | ./data | 各节点 WAL 落盘目录 |

## 一致性模型

- **法定人数（quorum）**：每次写必须收到 W 个副本确认、每次读必须收到 R 个
  副本响应，否则明确失败，**不会**悄悄返回旧值或部分结果：
  - 写确认不足 → `WRITE_QUORUM_NOT_MET`（HTTP 503）
  - 读响应不足 → `READ_QUORUM_NOT_MET`（HTTP 503）
  - 过期写被拒绝 → `STALE_CLOCK`（HTTP 409）
  - 检测到并发兄弟版本 → `CONFLICT`（HTTP 409，返回全部兄弟与合并上下文）
- **默认 W + R > N**（2 + 2 > 3）：任何一次成功读都能与最近一次成功写
  在副本集上相交，配合版本归并保证已提交数据**不回退**。
- **单键因果一致性**：向量时钟追踪每个键的版本谱系；客户端写入必须携带
  最近一次读到的 `context`，过期上下文的写会被拒绝，保护更新的已提交结果。
- **读修复（read repair）**：读请求把各副本的版本归并后回写到在线副本，
  落后/重新加入的副本在下一次读后被补齐，避免副本切换后读到旧值。
- **部分写（partial write）**：未达到 W 的写明确返回失败，但可能已在个别
  副本落盘；该值不会被当成已提交，后续读会把它作为并发兄弟版本以
  `CONFLICT` 暴露出来，由客户端解决——系统既不悄悄采纳也不悄悄丢弃，
  已提交的值不会被覆盖或回退。

## 分片策略

- 键空间按 `hash(key)` 均匀切成 `shard-count` 个区间（键范围分片）。
- 分片 `i` 的副本放在节点环上从第 `i` 个节点开始的连续 `replicas-per-shard`
  个节点，相邻分片的副本集错开，单个节点宕机只影响部分分片。

## 冲突处理策略

- 每个键的每个版本携带**向量时钟**；协调器为每次写分配新时钟
  （客户端读上下文 + 唯一写标记 `w:<opId>`）。
- 副本写入时逐版本比较：
  - 本地版本**更新**（支配新时钟）→ 拒绝本次写（`STALE_CLOCK`），
    过期副本数据不能覆盖更新的已提交结果；
  - 本地版本**更旧** → 被本次写取代；
  - **互不可比** → 并发写，保留为兄弟版本（siblings）。
- 读时归并所有副本的版本：被支配的版本丢弃，剩余多个并发版本则以
  `CONFLICT` 连同**合并上下文**一起返回；客户端解决冲突后携带该上下文
  重新写入，一次写即可取代全部兄弟版本。

## 故障与恢复

- 每次写先追加到本节点 WAL 并 `fsync`，再进内存；宕机只丢内存，不丢已提交数据。
- 节点重启（`up()`）时重放 WAL 恢复崩溃前状态；重放使用与读修复相同的
  归并语义，**重复重放幂等**，不会造成数据重复或版本错乱。
- 每次写携带客户端/协调器分配的 `opId`，副本记录已处理的 `opId`：
  同一操作重复提交或网络重试**幂等**，第二个相同 `opId` 直接视为已确认。
- 通过 admin 接口手动上下线节点，模拟宕机与网络分区。

## REST 接口

### 写入

```bash
curl -X PUT localhost:8080/kv/user:1 \
  -H 'Content-Type: application/json' \
  -d '{"value": "alice", "opId": "req-1", "context": null}'
# → {"status":"OK","context":{"w:req-1":1},"message":"ok"}
```

`opId` 缺省时服务端生成 UUID；`context` 为上一次读/写返回的上下文，
更新已存在的键时应携带，否则按盲写处理（可能产生并发兄弟版本）。

### 读取

```bash
curl localhost:8080/kv/user:1
# → {"status":"OK","values":[{"value":"alice","clock":{...},"tombstone":false}],
#    "context":{...},"message":"ok"}
```

冲突时返回 `409` 与全部兄弟版本；把响应里的 `context` 带回下一次写即可解决。

### 删除

```bash
curl -X DELETE localhost:8080/kv/user:1 \
  -H 'Content-Type: application/json' -d '{"opId":"req-2","context":{...}}'
```

### 节点管理（模拟宕机/分区）

```bash
curl -X POST localhost:8080/admin/nodes/node-1/down   # 下线
curl -X POST localhost:8080/admin/nodes/node-1/up     # 上线（重放 WAL 恢复）
curl localhost:8080/admin/nodes                       # 查看全部节点状态
curl localhost:8080/admin/nodes/node-1/keys/user:1    # 查看某节点本地版本
```

## 本地验证

### 跑测试

```bash
mvn test
```

测试日志按 `[业务:xxx][用例:yyy]` 格式打印每个场景的实际结果与期望，覆盖：

| 测试类 | 业务 | 用例 |
|---|---|---|
| `QuorumTest` | 读写法定人数 | 写/读达到法定人数、写不足（WRITE_QUORUM_NOT_MET）、读不足（READ_QUORUM_NOT_MET）、恰好法定人数、读不存在的键 |
| `ConflictTest` | 并发写冲突 | 并发写检测出兄弟版本、客户端合并上下文解决冲突、过期写拒绝覆盖已提交数据、顺序写无冲突 |
| `RecoveryTest` | 故障与恢复 | 重启从 WAL 恢复已提交状态、全集群重启数据不丢、重复提交幂等、WAL 多次重放幂等、过期副本重新加入不回退（读修复）、分区写入与愈合 |
| `ApiSmokeTest` | REST 端到端 | HTTP 写入/读取、admin 下线后写、恢复节点 |

### 手动验证

```bash
# 启动（默认 5 节点 / 3 分片 / N=3,W=2,R=2，数据落盘 ./data）
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080

# 写入并读取
curl -X PUT localhost:8080/kv/a -H 'Content-Type: application/json' -d '{"value":"1"}'
curl localhost:8080/kv/a

# 模拟宕机：关掉两个副本后写落在该分片的键 → 503 WRITE_QUORUM_NOT_MET
curl -X POST localhost:8080/admin/nodes/node-1/down
curl -X POST localhost:8080/admin/nodes/node-2/down

# 恢复节点（重放 WAL），数据不丢
curl -X POST localhost:8080/admin/nodes/node-1/up
curl -X POST localhost:8080/admin/nodes/node-2/up
```
