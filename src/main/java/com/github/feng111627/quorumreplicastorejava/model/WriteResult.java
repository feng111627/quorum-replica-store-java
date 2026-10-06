package com.github.feng111627.quorumreplicastorejava.model;

public record WriteResult(OpStatus status, Version version, String message) {

    public static WriteResult ok(Version version) {
        return new WriteResult(OpStatus.OK, version, "ok");
    }

    public static WriteResult fail(OpStatus status, String message) {
        return new WriteResult(status, null, message);
    }
}
