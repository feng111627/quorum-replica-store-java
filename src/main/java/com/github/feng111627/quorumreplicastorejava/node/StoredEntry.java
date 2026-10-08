package com.github.feng111627.quorumreplicastorejava.node;

import com.github.feng111627.quorumreplicastorejava.core.EntryState;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;

/**
 * 副本本地保存的单个版本：版本值 + 提交状态。
 * 状态在提交/放弃两阶段之间迁移，相同向量时钟的版本只保留一份。
 */
public record StoredEntry(VersionedValue version, EntryState state) {
    public static StoredEntry pending(VersionedValue v) {
        return new StoredEntry(v, EntryState.PENDING);
    }

    public static StoredEntry committed(VersionedValue v) {
        return new StoredEntry(v, EntryState.COMMITTED);
    }

    public StoredEntry withState(EntryState newState) {
        return new StoredEntry(version, newState);
    }
}
