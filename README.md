# Quorum Replica Store

本地可运行的分布式键值存储模拟：单进程内运行多个副本节点，每个节点用
**内存表 + 本地 WAL 落盘** 模拟独立机器的存储，支持分片、法定人数读写、
两阶段写（失败自动干净收场）、并发写冲突检测、节点故障与重启恢复、
只读一致性巡检与按需幂等收敛。

## 运行环境

- JDK 21+（开发验证使用 OpenJDK 25）
- Maven 3.9+

## 架构

```
客户端
  │  HTTP
  ▼
KvController / AdminController          (api：读写 / 节点管理 / 巡检 / 收敛)
  │
  ▼
Coordinator  ── 两阶段写、W/R 法定人数判定、版本归并、读修复、巡检、收敛  (cluster)
  │            └─ coordinator-decision.log（提交/放弃决定，fsync 落盘）
  ▼
NodeRegistry ── ShardRouter（键范围分片 + 节点环副本放置）     (cluster/core)
  │
  ▼
ReplicaNode × N ── 带提交状态的版本表（PENDING/COMMITTED/ABORTED）+ WAL v2  (node)
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
- **两阶段写，失败即回滚（当作从没发生）**：写先在各副本落 `PENDING` 预备值，
  凑够 W 个确认后，协调器先把 COMMIT 决定 fsync 到决定日志，再让副本转
  `COMMITTED`；凑不够则先 fsync ABORT 决定，再通知所有在线副本放弃。保证：
  - 未凑够确认的写**任何副本都不会读出来**：读路径只认 COMMITTED，
    `PENDING`/`ABORTED` 既不返回也不会被读修复带到别的副本；
  - 同一键读到的是失败写**之前**的状态（新键就是 `NOT_FOUND`，删除写失败
    旧值仍在），客户端不需要处理任何“中间态”；
  - 节点宕机重上线（重放自己的 WAL）、或服务整体重启（重放副本 WAL +
    协调器决定日志）后，残留也**不会复活**；收敛时的物理回收同样落盘（GC 记录）。
  - 接口上失败写就是一次普通失败（`WRITE_QUORUM_NOT_MET`/`STALE_CLOCK`），
    不存在需要人工处理的第三种状态。
- **默认 W + R > N**（2 + 2 > 3）：任何一次成功读都能与最近一次成功写
  在副本集上相交，配合版本归并保证已提交数据**不回退**。
- **单键因果一致性**：向量时钟追踪每个键的版本谱系；客户端写入必须携带
  最近一次读到的 `context`，过期上下文的写会被拒绝，保护更新的已提交结果。
- **读修复（read repair）**：读请求只把**已提交**版本归并后回写到在线副本，
  落后/重新加入的副本在下一次读后被补齐；未提交的预备值绝不参与传播。
- **同一 opId 重试幂等**：opId 决定向量时钟，重试直接返回已 fsync 的决定，
  已放弃的写不会因为重试被重新提交。

## 分片策略

- 键空间按 `hash(key)` 均匀切成 `shard-count` 个区间（键范围分片）。
- 分片 `i` 的副本放在节点环上从第 `i` 个节点开始的连续 `replicas-per-shard`
  个节点，相邻分片的副本集错开，单个节点宕机只影响部分分片。

## 冲突处理策略

- 每个键的每个版本携带**向量时钟**；协调器为每次写分配新时钟
  （客户端读上下文 + 唯一写标记 `w:<opId>`）。
- 预备阶段只与本地**已提交**版本比较，且不取代任何本地数据：
  - 本地已提交版本**更新**（支配新时钟）→ 拒绝本次写（`STALE_CLOCK`），
    过期数据不能覆盖更新的已提交结果；
  - 其余情况落 `PENDING`，与旧版本并存。
- 提交决定到达时才归并：被新提交支配的旧已提交版本被取代，
  **互不可比**的并发已提交版本保留为兄弟版本（siblings）。
- 读时只归并各副本的**已提交**版本：被支配的版本丢弃，剩余多个并发版本则以
  `CONFLICT` 连同**合并上下文**一起返回；客户端解决冲突后携带该上下文
  重新写入，一次写即可取代全部兄弟版本。
- `PENDING`/`ABORTED` 版本不参与上述任何归并，因此失败写永远不会被
  误报成并发冲突。

## 故障与恢复

- WAL v2 以 `QRS1` 文件头 + 带类型记录落盘：`PREPARE`（预备值）、`COMMIT`
  （提交决定，携带完整值）、`ABORT`（放弃决定）、`GC_ABORT`（残留已物理回收）、
  `DECISION`（协调器全局决定，含 tombstone）。每条记录追加后 fsync，
  尾部半条记录会被容忍。**旧版无文件头日志仍可读取**，旧记录一律视为已提交数据。
- 节点重启（`up()`）时重放 WAL：PREPARE 回到 PENDING，COMMIT/ABORT 迁移状态，
  GC 记录保持回收结果——重复重放幂等，不会造成数据重复、版本错乱或残留复活。
- 协调器把每次写的最终决定（提交/放弃）追加到 `coordinator-decision.log` 并
  fsync 后才响应客户端；整机重启先重放该日志，保证“失败写不复活”不依赖某个副本。
- 每次写携带客户端/协调器分配的 `opId`，副本记录已处理的 `opId`：
  同一操作重复提交或网络重试**幂等**。
- 通过 admin 接口手动上下线节点，模拟宕机与网络分区。

## 一致性巡检（只读）与按需收敛

### 巡检（inspect，绝不修改数据）

- `GET /admin/inspect/keys/{key}`：收集**全部副本（含下线副本）**上该键的版本与
  提交状态，逐副本给出差异分类，并汇总未提交残留：

  | kind | 含义 |
  |---|---|
  | `CONSISTENT` | 与集群已提交目标版本一致（新键时各副本无数据也算一致） |
  | `MISSING_COMMITTED` | **正常的暂时落后**：提交完成了，但该副本还没收到任何目标版本 |
  | `COMMITTED_LAGGING` | **正常的暂时落后**：副本有部分已提交版本，还缺较新的目标版本 |
  | `UNCOMMITTED_RESIDUE` | **没提交干净的残留**：存在 `PENDING`/`ABORTED` 版本（见 `residueVersions`） |
  | `NODE_DOWN` | 副本下线，无法观察 |

- `GET /admin/inspect`：对所有在线副本持有的键（及决定日志中的键）逐个巡检，
  返回 `keyCount`、`inconsistentKeys` 与每键明细。巡检只读，重复执行结果不变。

### 收敛（repair，可反复执行、结果幂等）

- `POST /admin/repair/keys/{key}` / `POST /admin/repair`：
  - 把已提交目标版本（归并后，含并发兄弟与 tombstone）幂等补齐到在线副本；
  - 对残留：有 COMMIT 决定的预备值正常提交；**没有提交决定的 PENDING 一律按
    放弃处理**（先补一条持久化 ABORT 决定再物理回收），因此**绝不会把没凑够
    确认的写变成大家都能读到的数据**；
  - 返回 `changed`、补齐/清理的副本计数与每个时钟的归宿（`decisions`）。
- 幂等性：第二次对同一状态再收敛 `changed=false`；中途重复触发、整机重启后再
  收敛都不会搞乱版本（GC 记录保证重启后残留不重现）。
- 下线副本不会被触碰；其上线后再次执行收敛即可补齐。
- 收敛不“解决”业务上的并发冲突：两个已提交兄弟会被一致地复制到所有副本，
  读仍返回 `CONFLICT`，由客户端携带合并上下文写入最终值。

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

### 节点管理（模拟宕机/分区）与本地版本视图

```bash
curl -X POST localhost:8080/admin/nodes/node-1/down   # 下线
curl -X POST localhost:8080/admin/nodes/node-1/up     # 上线（重放 WAL 恢复）
curl localhost:8080/admin/nodes                       # 查看全部节点状态
curl localhost:8080/admin/nodes/node-1/keys/user:1    # 查看某节点本地版本（含 state）
```

本地版本视图每项带 `state`：`PENDING`（未确认预备值）/`COMMITTED`（可读）/
`ABORTED`（已放弃残留）。

### 只读一致性巡检

```bash
curl localhost:8080/admin/inspect/keys/user:1         # 单键：逐副本 kind/detail + residueVersions
curl localhost:8080/admin/inspect                     # 整库：keyCount / inconsistentKeys
```

### 按需收敛（幂等，可重复触发）

```bash
curl -X POST localhost:8080/admin/repair/keys/user:1  # 收敛单键
curl -X POST localhost:8080/admin/repair              # 收敛整库
# → {"key":...,"changed":false,"committedPropagated":0,"residuePurged":0,"decisions":[...],"summary":...}
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
| `FailedWriteRollbackTest` | 失败写干净收场 | 新键失败写不可读且残留为 ABORTED、失败写不覆盖旧值、承载节点重启不复活、整机重启不复活、失败 opId 重试不复活、成功写与顺序更新不受影响、残留不阻塞后续成功写、失败删除不隐藏旧值、下线副本经读修复拿到完整值 |
| `InspectionTest` | 只读一致性巡检 | 一致键判定、落后副本定性（MISSING/LAGGING）、未提交残留与落后区分、下线副本呈现、整库汇总、巡检只读不改数据 |
| `RepairConvergenceTest` | 按需收敛 | 补齐落后副本、清除残留且不提升 ghost、残留清除后仍是旧值、重复/中途再跑幂等、不破坏并发已提交兄弟、下线副本上线后再收敛、整库收敛二次无改动、整机重启后仍拒绝 ghost、回收后重启巡检仍一致 |
| `ReplicaNodeTwoPhaseTest` | 节点两阶段原语 | PENDING 不可见、COMMIT/ABORT 幂等、带值提交不留空壳、ABORT 经 WAL 重放持久、下线拒绝一切操作 |
| `WalCompatibilityTest` | WAL 格式兼容 | 旧版无文件头日志按已提交数据恢复、不被误判为残留 |
| `ApiSmokeTest` | REST 端到端 | HTTP 写入/读取、admin 下线后写、恢复节点、巡检/收敛接口、本地版本 state 视图 |

### 手动验证

```bash
# 启动（默认 5 节点 / 3 分片 / N=3,W=2,R=2，数据落盘 ./data）
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8080"

# 1) 写入并读取
curl -X PUT localhost:8080/kv/a -H 'Content-Type: application/json' \
  -d '{"value":"1","opId":"m1","context":null}'
curl localhost:8080/kv/a

# 2) 看 a 的三个副本
curl localhost:8080/admin/inspect/keys/a

# 3) 关掉其中两个副本后再写 → 503 WRITE_QUORUM_NOT_MET
curl -X POST localhost:8080/admin/nodes/node-1/down
curl -X POST localhost:8080/admin/nodes/node-2/down
curl -X PUT localhost:8080/kv/a -H 'Content-Type: application/json' \
  -d '{"value":"ghost","opId":"m2","context":{"w:m1":1}}'

# 4) 恢复节点：读仍是旧值，ghost 永不出现；巡检把残留标为 UNCOMMITTED_RESIDUE
curl -X POST localhost:8080/admin/nodes/node-1/up
curl -X POST localhost:8080/admin/nodes/node-2/up
curl localhost:8080/kv/a
curl localhost:8080/admin/inspect/keys/a

# 5) 按需收敛（再调一次 changed 必为 false）
curl -X POST localhost:8080/admin/repair/keys/a
curl localhost:8080/admin/inspect
curl -X POST localhost:8080/admin/repair
```

## 兼容范围

- WAL：保留旧版（无 `QRS1` 头）日志的读取能力，旧记录按已提交数据恢复；
  新记录一律 v2 格式。旧版程序无法读取 v2 日志，升级后不需要手工迁移。
- REST：读写接口请求/响应不变；本地版本视图新增 `state` 字段；
  新增 `/admin/inspect*` 与 `/admin/repair*`。`data-dir` 下新增
  `coordinator-decision.log`。
- 语义变化：未达法定人数的写不再以 `CONFLICT` 兄弟形式暴露给客户端，
  而是彻底回滚为“从未发生”；如需查看个别副本上的残留，请用巡检接口。
