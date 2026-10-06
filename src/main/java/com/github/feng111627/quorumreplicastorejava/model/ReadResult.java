package com.github.feng111627.quorumreplicastorejava.model;

public record ReadResult(OpStatus status, VersionedValue value, String message) {

    public static ReadResult ok(VersionedValue value) {
        return new ReadResult(OpStatus.OK, value, "ok");
    }

    public static ReadResult fail(OpStatus status, String message) {
        return new ReadResult(status, null, message);
    }
}
