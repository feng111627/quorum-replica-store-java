package com.github.feng111627.quorumreplicastorejava.core;

import java.util.List;

/** 读写结果：区分成功 / 法定人数不足 / 冲突等不同失败原因。 */
public final class Results {
    private Results() {}

    public enum Status {
        OK,                     // 成功
        NOT_FOUND,              // 键不存在
        WRITE_QUORUM_NOT_MET,   // 写确认数不足
        READ_QUORUM_NOT_MET,    // 读确认数不足
        CONFLICT,               // 检测到并发写冲突（存在兄弟版本）
        STALE_CLOCK             // 携带的上下文过期，拒绝覆盖更新的已提交数据
    }

    public record ValueView(String value, VectorClock clock, boolean tombstone) {
        public static ValueView of(VersionedValue v) {
            return new ValueView(v.value(), v.clock(), v.tombstone());
        }
    }

    public record ReadResult(Status status, List<ValueView> values, VectorClock context, String message) {
        public static ReadResult ok(ValueView v, VectorClock ctx) {
            return new ReadResult(Status.OK, List.of(v), ctx, "ok");
        }
        public static ReadResult notFound(VectorClock ctx) {
            return new ReadResult(Status.NOT_FOUND, List.of(), ctx, "key not found");
        }
        public static ReadResult conflict(List<ValueView> siblings, VectorClock ctx) {
            return new ReadResult(Status.CONFLICT, siblings, ctx,
                    "concurrent writes detected, resolve with returned context");
        }
        public static ReadResult quorumNotMet(int got, int need) {
            return new ReadResult(Status.READ_QUORUM_NOT_MET, List.of(), null,
                    "read quorum not met: got " + got + " of " + need);
        }
    }

    public record WriteResult(Status status, VectorClock context, String message) {
        public static WriteResult ok(VectorClock ctx) {
            return new WriteResult(Status.OK, ctx, "ok");
        }
        public static WriteResult quorumNotMet(int got, int need) {
            return new WriteResult(Status.WRITE_QUORUM_NOT_MET, null,
                    "write quorum not met: got " + got + " of " + need);
        }
        public static WriteResult staleClock() {
            return new WriteResult(Status.STALE_CLOCK, null,
                    "stale context: rejected to protect newer committed data");
        }
    }
}
