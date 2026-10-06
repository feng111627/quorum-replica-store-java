package com.github.feng111627.quorumreplicastorejava.model;

/**
 * 带版本的值。opId 用于幂等去重：同一操作重放/重试不会产生新版本。
 */
public record VersionedValue(String value, Version version, String opId) {
}
