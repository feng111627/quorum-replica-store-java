# Quorum Replica Store（本地模拟分布式键值存储）

基于 Spring Boot 的分布式键值存储模拟实现：单进程内运行多个副本节点，
每个节点用内存表 + 本地 WAL 文件落盘，模拟多副本、分片、法定人数读写、
冲突检测、节点故障与重启恢复的完整语义。

## 一致性模型

- **法定人数（Quorum）一致性**：每个分片 N=3 个副本，写需 W=2 个副本确认，
  读需 R=2 个副本应答。W + R > N 保证读写法定人数必然相交，
  已提交的数据对后续读取可见，不会出现已提交数据往回退。
- **版本化数据**：每个键的值携带版本号 `Version(sequence, writerId)`，
  全局字典序可比。读取返回法定人数应答中的最大版本。
- **读修复（Read Repair）**：读取时把最大版本回写给落后副本；
  节点侧有版本检查，过期数据永远无法覆盖更新的已提交结果。
- **已提交不回退**：节点重启后从 WAL 恢复到崩溃前状态；
  落后副本重新上线后，读取仍返回最新已提交值，并被读修复推进。

## 法定人数规则

| 场景 | 行为 |
| --- | --- |
| 写确认数 ≥ W | 返回 `OK` 与新版本 |
| 写确认数 < W | **回滚**已确认副本上的本次写入（失败写不留痕迹），返回 `WRITE_QUORUM_NOT_MET`（HTTP 503） |
| 读应答数 ≥ R | 返回最大版本的值 |
| 读应答数 < R | 返回 `READ_QUORUM_NOT_MET`（HTTP 503），绝不悄悄返回旧值或部分结果 |
| 键不存在 | 返回 `KEY_NOT_FOUND`（HTTP 404） |

## 分片策略

- 键空间按 `hash(key) mod shardCount` 切成固定数量分片（默认 3 个）。
- 每个分片映射到一组独立的副本节点（默认每分片 3 个，共 9 个节点）。
- 分片布局由 `ShardLayout` 统一管理，协调器按键定位分片再路由到副本组。

## 冲突处理策略

- **乐观并发控制（compare-and-swap）**：写入时客户端携带 `baseVersion`
  （读到的版本；新键传 null）。协调器校验 `baseVersion` 必须等于当前
  已提交的最大版本，不一致则返回 `CONFLICT`（HTTP 409）。
  两个并发写基于同一版本时，先到者提交，后到者收到冲突，已提交值不被覆盖。
- **幂等提交**：每次写入携带唯一 `opId`。同一 `opId` 的重试/重放在协调器
  和副本节点两层都会被识别为已提交，直接返回原结果，不产生重复数据或新版本。
- **版本有序写入**：副本节点拒绝版本 ≤ 当前版本的写入（不同 opId），
  保证过期副本数据不会覆盖更新的已提交结果。

## 故障与恢复

- 每个节点在 `store.data-dir`（默认 `./data`）下维护独立的追加式 WAL
  （`<nodeId>.wal`），先落盘再改内存。
- WAL 记录两类条目：`PUT`（写入）与 `ROLLBACK`（撤销指定 opId 的写入并
  恢复之前状态）。重启时按序重放，恢复到崩溃前的已提交状态。
- 通过管理接口手动上/下线节点，模拟宕机与网络分区。

## HTTP 接口

### 读写

```bash
# 写入（baseVersion 可选，opId 可选——不传则自动生成）
curl -X PUT localhost:8080/kv/k1 -H 'Content-Type: application/json' \
     -d '{"value":"hello","baseVersion":null,"opId":"op-1"}'
# => {"status":"OK","version":"1@coordinator"}

# 读取
curl localhost:8080/kv/k1
# => {"status":"OK","value":"hello","version":"1@coordinator"}
```

### 节点管理（模拟故障）

```bash
curl localhost:8080/admin/nodes                       # 查看所有节点状态
curl -X POST localhost:8080/admin/nodes/shard0-node0/down     # 下线（模拟宕机/分区）
curl -X POST localhost:8080/admin/nodes/shard0-node0/up       # 上线
curl -X POST localhost:8080/admin/nodes/shard0-node0/restart  # 崩溃重启：从 WAL 恢复
```

## 本地验证

### 运行单测（日志可看到每个业务场景的用例与结果）

```bash
mvn test
```

`QuorumStoreTests` 覆盖 9 个业务场景，日志中带 `[用例]` / `[结果]` 标记：

| 用例 | 验证点 |
| --- | --- |
| 法定人数读写-正常路径 | W=2/R=2 下写入可读回 |
| 写法定人数不足 | 下线 2/3 副本 → `WRITE_QUORUM_NOT_MET` |
| 读法定人数不足 | 下线 2/3 副本 → `READ_QUORUM_NOT_MET`，不返回旧值 |
| 并发写冲突 | 同基线版本并发写 → 后到者 `CONFLICT` |
| 过期副本数据不得覆盖已提交结果 | 节点拒绝旧版本写入 |
| 节点宕机重启恢复 | WAL 重放恢复到崩溃前状态 |
| 重复提交/重放幂等 | 同 opId 重试/重放不产生重复或版本错乱 |
| 恢复后读取不回退 | 落后副本上线后读取仍为最新已提交值 |
| 写法定人数不足回滚 | 失败写不留痕迹，恢复后读不到 |

### 手动端到端验证

```bash
# 启动（可改端口与数据目录）
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8080 --store.data-dir=./data"

# 正常读写
curl -X PUT localhost:8080/kv/k1 -H 'Content-Type: application/json' -d '{"value":"hello","opId":"op-1"}'
curl localhost:8080/kv/k1   # 注意端口，示例见下

# 模拟分片内多数副本宕机（k1 落在 shard0）
curl -X POST localhost:8080/admin/nodes/shard0-node0/down
curl -X POST localhost:8080/admin/nodes/shard0-node1/down
curl localhost:8080/kv/k1    # => 503 READ_QUORUM_NOT_MET

# 重启恢复
curl -X POST localhost:8080/admin/nodes/shard0-node0/restart
curl -X POST localhost:8080/admin/nodes/shard0-node1/restart
curl localhost:8080/kv/k1    # => 仍返回已提交的值，无回退
```

## 配置项（application.properties / 启动参数）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `store.data-dir` | `./data` | 各节点 WAL 落盘目录 |
| `store.shard-count` | `3` | 分片数量 |
| `store.replicas-per-shard` | `3` | 每分片副本数（N） |
| `store.write-quorum` | `2` | 写法定人数（W） |
| `store.read-quorum` | `2` | 读法定人数（R） |

## 代码结构

```
model/      Version、VersionedValue、OpStatus、ReadResult/WriteResult
node/       ReplicaNode（内存表+WAL+上下线）、WriteAheadLog（PUT/ROLLBACK 追加日志）
shard/      ShardLayout（键→分片→副本组）
service/    CoordinatorService（法定人数读写、冲突检测、回滚、读修复）
web/        KvController（读写）、AdminController（节点上下线/重启）
config/     StoreConfig（组装 3 分片 × 3 副本的本地集群）
```
