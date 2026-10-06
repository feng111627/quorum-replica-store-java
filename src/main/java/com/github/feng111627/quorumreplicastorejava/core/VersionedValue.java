package com.github.feng111627.quorumreplicastorejava.core;

/** 带版本（向量时钟）的值，tombstone 表示已删除。 */
public record VersionedValue(String value, VectorClock clock, boolean tombstone) {
    public static VersionedValue of(String value, VectorClock clock) {
        return new VersionedValue(value, clock, false);
    }

    public static VersionedValue tombstone(VectorClock clock) {
        return new VersionedValue(null, clock, true);
    }
}
