package com.github.feng111627.quorumreplicastorejava.node;

import com.github.feng111627.quorumreplicastorejava.model.VersionedValue;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 副本节点：内存表 + 本地 WAL 落盘。可被标记上/下线以模拟宕机与分区。
 * 写入语义：
 *  - 幂等：相同 opId 重复提交/重放直接视为成功，不产生重复或新版本；
 *  - 版本有序：过期版本（小于等于当前版本）不得覆盖更新的已提交数据；
 *  - 可回滚：协调器在写法定人数不足时撤销已落盘的写入，失败操作不留痕迹。
 */
public class ReplicaNode {

    private final String nodeId;
    private final Path dataDir;
    private final ConcurrentHashMap<String, VersionedValue> store = new ConcurrentHashMap<>();
    private volatile boolean up = true;
    private WriteAheadLog wal;

    public ReplicaNode(String nodeId, Path dataDir) {
        this.nodeId = nodeId;
        this.dataDir = dataDir;
        this.wal = new WriteAheadLog(dataDir.resolve(nodeId + ".wal"));
    }

    public String id() {
        return nodeId;
    }

    public boolean isUp() {
        return up;
    }

    public void setUp(boolean up) {
        this.up = up;
    }

    /** 崩溃后重启：清空内存，从 WAL 重放（含回滚记录）恢复到崩溃前已提交状态。 */
    public synchronized void recover() {
        store.clear();
        wal.close();
        wal = new WriteAheadLog(dataDir.resolve(nodeId + ".wal"));
        wal.replay(new WriteAheadLog.ReplayVisitor() {
            @Override
            public void onPut(String key, VersionedValue value) {
                applyToMemory(key, value);
            }

            @Override
            public void onRollback(String key, String opId, VersionedValue previous) {
                applyRollbackToMemory(key, opId, previous);
            }
        });
    }

    /**
     * 写入副本。返回 true 表示该副本已确认（含幂等命中）；
     * 返回 false 表示该写入版本过期被拒绝。
     */
    public synchronized boolean put(String key, VersionedValue value) {
        ensureUp();
        VersionedValue current = store.get(key);
        if (current != null) {
            if (current.opId().equals(value.opId())) {
                return true; // 同一操作重试/重放：幂等成功
            }
            if (value.version().compareTo(current.version()) <= 0) {
                return false; // 过期数据不得覆盖更新的已提交结果
            }
        }
        wal.appendPut(key, value); // 先落盘再改内存
        store.put(key, value);
        return true;
    }

    /**
     * 撤销指定 opId 的写入并恢复之前的状态（previous 为 null 表示之前无此键）。
     * 仅当当前值确实由该 opId 写入时才回滚，避免误撤销更新的已提交数据。
     */
    public synchronized boolean rollback(String key, String opId, VersionedValue previous) {
        ensureUp();
        VersionedValue current = store.get(key);
        if (current == null || !current.opId().equals(opId)) {
            return false; // 该副本上没有这次写入，或已被更新的提交覆盖
        }
        wal.appendRollback(key, opId, previous);
        applyRollbackToMemory(key, opId, previous);
        return true;
    }

    public Optional<VersionedValue> get(String key) {
        ensureUp();
        return Optional.ofNullable(store.get(key));
    }

    public Map<String, VersionedValue> snapshot() {
        ensureUp();
        return Collections.unmodifiableMap(new HashMap<>(store));
    }

    private void applyToMemory(String key, VersionedValue value) {
        VersionedValue current = store.get(key);
        if (current == null || value.version().compareTo(current.version()) > 0) {
            store.put(key, value);
        }
    }

    private void applyRollbackToMemory(String key, String opId, VersionedValue previous) {
        VersionedValue current = store.get(key);
        if (current != null && current.opId().equals(opId)) {
            if (previous == null) {
                store.remove(key);
            } else {
                store.put(key, previous);
            }
        }
    }

    private void ensureUp() {
        if (!up) {
            throw new IllegalStateException("节点 " + nodeId + " 已下线");
        }
    }
}
