package com.github.feng111627.quorumreplicastorejava.model;

/**
 * 操作结果状态：调用方可以据此区分失败原因，而不是拿到含糊的旧值或部分结果。
 */
public enum OpStatus {
    OK,
    KEY_NOT_FOUND,
    WRITE_QUORUM_NOT_MET,
    READ_QUORUM_NOT_MET,
    CONFLICT,
    NODE_UNAVAILABLE
}
