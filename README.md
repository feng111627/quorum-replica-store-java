# Quorum Replica Store

本地可运行的分布式键值存储模拟：单进程内运行多个副本节点，每个节点用
**内存表 + 本地 WAL 落盘** 模拟独立机器的存储，支持分片、法定人数读写、
并发写冲突检测、节点故障与重启恢复；写失败（未凑够法定人数）按两阶段
协议**干净撤销、当作从未发生**，并提供**只读一致性巡检**与**按需收敛**。

## 运行环境

- JDK 21+（开发验证使用 OpenJDK 25）
- Maven 3.9+

## 架构

```
客户端
  │  HTTP
  ▼
KvController / AdminController                        (api)
  │
  ▼
Coordinator  ── 两阶段写、W/R 法定人数判定、版本归并、
  │             读修复、只读巡检、按需收敛              (cluster)
  ▼
NodeRegistry ── ShardRouter（键范围分片 + 节点环副本放置）(cluster/core)
  │
  ▼
ReplicaNode × N ── 已提交表 + 未提交(pending)区 + Wal   (node)
                   （追加落盘 + fsync，PUT/COMMIT/ABORT 三类记录）
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
- **读修复（read repair）**：读请求把各副本的**已提交**版本归并后回写到在线
  副本，落后/重新加入的副本在下一次读后被补齐；读修复只搬运已提交版本，
  不会传播任何未提交残留。

## 写失败的真实行为（两阶段写）

写入分两个阶段：

1. **prepare**：协调器把值发到所有副本，副本写入本地 **pending 区**
   （WAL 记录 `PUT(committed=false)` + fsync）。pending 区的值**对读不可见**，
   读路径只看已提交表。
2. **commit / abort**：
   - 凑够 W 个确认 → 向所有副本发 **commit**（WAL 记录 `COMMIT`），
     pending 版本并入已提交表，对读可见，返回 `OK`；
   - 凑不够 → 向所有副本发 **abort**（WAL 记录 `ABORT`），pending 版本被
     删除，返回 `WRITE_QUORUM_NOT_MET`。

因此一次失败的写**当作从未发生**：

- 之后任何副本都不会把它作为可读数据返回；同一个键读到的是失败写之前的
  已提交状态，新键就是 `NOT_FOUND`；
- 它不会作为"并发兄弟版本"被暴露给客户端，也不需要客户端做任何清理；
- 副本重新上线、服务整体重启后它也不会复活（`ABORT` 记录已落盘，重放时
  同样撤销）；
- 失败写的 `opId` 在副本上被解除标记，客户端可以用**同一个 opId** 干净地
  重试，重试就是一次全新的写。

边界说明（单进程模拟的假设）：单次写期间协调器自身不宕机、副本上下线状态
不在写的过程中变化。若副本在 prepare 之后、abort 到达之前宕机，它盘上会留
下一条未被撤销的 pending 记录——这就是"未提交残留"，它对读永远不可见，
可以被**巡检**发现、被**收敛**清除（见下两节）。

## 只读一致性巡检

巡检把各副本上某个键（或整个库）的版本收集出来对比，**不改动任何数据**：

```bash
curl 'localhost:8080/admin/inspect?key=user:1'   # 巡检单个键
curl 'localhost:8080/admin/inspect'              # 巡检整个库
```

每个键的诊断（`diagnosis`）区分四类：

| 诊断 | 含义 |
|---|---|
| `CONSISTENT` | 所有在线副本的已提交版本一致，且无未提交残留 |
| `LAGGING` | 部分副本缺少已提交版本——**正常提交完成后暂时落后**，读修复或收敛可补齐 |
| `UNCOMMITTED_RESIDUE` | 副本上存在**未凑够法定人数的写的残留**（pending），对读不可见但应清理 |
| `MIXED` | 既有副本落后又有未提交残留 |

报告同时给出 `laggingNodes`（落后的具体副本）、`residueNodes`（持残留的
具体副本）、`downNodes`（下线未采集的副本）以及每个副本的已提交/未提交
版本明细（值、向量时钟、产生残留的 opId），说明"哪些副本对不上、差在哪里"。

## 按需收敛

巡检发现不一致后，可触发收敛把它修到一致：

```bash
curl -X POST 'localhost:8080/admin/converge?key=user:1'   # 收敛单个键
curl -X POST 'localhost:8080/admin/converge'              # 收敛整个库
```

收敛对每个键做两件事：

1. 把各在线副本的**已提交**版本归并为目标状态，回写到所有在线副本
   （补齐落后者；副本无变化时不落盘）；
2. 清除所有未提交残留（对每个残留写 `ABORT` 记录，重启后不会复活）。

保证：

- **幂等、可反复执行**：重复触发或中途再跑一遍结果相同，版本不会被搞乱；
  已一致的键再次收敛报告 `already converged, no action taken`；
- **绝不提升未提交的写**：即使所有副本都持有同一个未提交值（例如协调器在
  prepare 后宕机），收敛也只会清除它，绝不会把它变成可读数据；
- **不替客户端解决冲突**：已提交的并发兄弟版本原样保留，读仍返回
  `CONFLICT`，冲突解决永远是客户端的职责；
- 下线的副本不参与本次收敛，重新上线后由读修复或下一次收敛补齐。

## 分片策略

- 键空间按 `hash(key)` 均匀切成 `shard-count` 个区间（键范围分片）。
- 分片 `i` 的副本放在节点环上从第 `i` 个节点开始的连续 `replicas-per-shard`
  个节点，相邻分片的副本集错开，单个节点宕机只影响部分分片。

## 冲突处理策略

- 每个键的每个版本携带**向量时钟**；协调器为每次写分配新时钟
  （客户端读上下文 + 唯一写标记 `w:<opId>`）。
- 副本 prepare 时与本地**已提交**版本比较：
  - 本地已提交版本**更新**（支配新时钟）→ 拒绝本次写（`STALE_CLOCK`），
    过期副本数据不能覆盖更新的已提交结果；
  - 其他情况 → 接受 prepare，提交时按归并语义并入：被支配的旧版本被取代，
    **互不可比**的保留为兄弟版本（siblings）。
- 读时归并所有副本的已提交版本：被支配的版本丢弃，剩余多个并发版本则以
  `CONFLICT` 连同**合并上下文**一起返回；客户端解决冲突后携带该上下文
  重新写入，一次写即可取代全部兄弟版本。

## 故障与恢复

- 每次状态变化先追加到本节点 WAL 并 `fsync`，再进内存；宕机只丢内存。
- WAL 记录三类条目：`PUT`（带 committed 标志的写）、`COMMIT`（提交某个
  prepare 过的操作）、`ABORT`（撤销某个 prepare 过的操作）。
- 节点重启（`up()`）时重放 WAL 恢复崩溃前状态：已提交的回到已提交表，
  被撤销的不复存在，重放使用与运行时相同的状态转移函数，**重复重放幂等**。
- 每次写携带客户端/协调器分配的 `opId`：同一操作重复提交或网络重试**幂等**；
  被 abort 的 opId 会解除标记，允许客户端用同一 opId 干净重试。
- 通过 admin 接口手动上下线节点，模拟宕机与网络分区。

## 兼容范围

- **REST 协议**：读写接口的请求/响应结构与状态码和上一版一致，客户端无需
  修改；写失败对客户端的可见变化是：失败后读到的是写前状态/`NOT_FOUND`，
  而**不再**把失败写的值作为 `CONFLICT` 兄弟版本暴露（旧行为需要客户端
  手工清理，新行为由系统保证干净）。
- **WAL 磁盘格式**：v2（带魔数/版本头与 PUT/COMMIT/ABORT 记录类型）与
  v1 **不兼容**。用旧数据目录启动会在重放时报明确错误；升级前请清空
  `cluster.data-dir`（或先自行迁移数据）。这是本地模拟项目，不提供在线迁移。
- **行为假设**：单进程模拟，单次写/收敛期间协调器不宕机、副本上下线状态
  不在操作进行中变化；跨操作的宕机/分区/重启均在支持范围内。

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
curl localhost:8080/admin/nodes/node-1/keys/user:1    # 查看某节点本地版本（committed + pending）
```

### 一致性巡检与收敛

```bash
curl 'localhost:8080/admin/inspect?key=user:1'   # 只读巡检单键
curl 'localhost:8080/admin/inspect'              # 只读巡检全库
curl -X POST 'localhost:8080/admin/converge?key=user:1'   # 收敛单键
curl -X POST 'localhost:8080/admin/converge'              # 收敛全库
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
| `FailedWriteTest` | 写失败清理 | 失败写后新键查不到、不扰动已提交值、全集群重启后不复活、不与后续成功写冲突、同 opId 干净重试、失败删除不生效 |
| `InspectionTest` | 一致性巡检 | 一致键判定、落后副本识别（区别于残留）、未提交残留识别（区别于落后）、巡检只读不改数据、全库巡检覆盖所有键 |
| `ConvergenceTest` | 按需收敛 | 修复落后副本、清除残留且不可读、全副本持残留也绝不提升、幂等可重复、保留已提交冲突兄弟、全库收敛且重启后残留不复活 |
| `ApiSmokeTest` | REST 端到端 | HTTP 写入/读取、admin 下线后写、恢复节点 |

### 手动验证

```bash
# 启动（默认 5 节点 / 3 分片 / N=3,W=2,R=2，数据落盘 ./data）
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080

# 写入并读取
curl -X PUT localhost:8080/kv/a -H 'Content-Type: application/json' -d '{"value":"1"}'
curl localhost:8080/kv/a

# 模拟宕机：关掉 node-1/node-2 后，键 c 落在分片 0（副本 node-1/2/3），
# 只剩 1 个副本可写 → 503 WRITE_QUORUM_NOT_MET
curl -X POST localhost:8080/admin/nodes/node-1/down
curl -X POST localhost:8080/admin/nodes/node-2/down
curl -X PUT localhost:8080/kv/c -H 'Content-Type: application/json' -d '{"value":"x"}'

# 恢复节点后：失败的写当作从未发生（查不到），成功写的数据不丢
curl -X POST localhost:8080/admin/nodes/node-1/up
curl -X POST localhost:8080/admin/nodes/node-2/up
curl localhost:8080/kv/c        # → NOT_FOUND（失败写不留痕迹）
curl localhost:8080/kv/a        # → OK（已提交数据不丢）

# 巡检整个库，发现不一致后按需收敛
curl 'localhost:8080/admin/inspect'
curl -X POST 'localhost:8080/admin/converge'
curl 'localhost:8080/admin/inspect'   # 再次巡检应全部 CONSISTENT
```
