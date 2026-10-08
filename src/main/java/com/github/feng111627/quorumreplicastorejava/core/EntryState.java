package com.github.feng111627.quorumreplicastorejava.core;

/**
 * 一个版本在单个副本上的提交状态：
 * PENDING   —— 两阶段写的预备值，尚未凑够法定人数确认，读路径绝不返回；
 * COMMITTED —— 已凑够法定人数并由协调器落盘提交决定，是唯一可读的数据；
 * ABORTED   —— 协调器已落盘放弃决定的写，仅保留标记防止旧值复活，永不返回。
 */
public enum EntryState {
    PENDING,
    COMMITTED,
    ABORTED
}
