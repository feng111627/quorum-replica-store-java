package com.github.feng111627.quorumreplicastorejava.model;

/**
 * 版本号：逻辑序列号 + 写入者标识，字典序比较，保证全局可排序。
 */
public record Version(long sequence, String writerId) implements Comparable<Version> {

    @Override
    public int compareTo(Version other) {
        int c = Long.compare(this.sequence, other.sequence);
        return c != 0 ? c : this.writerId.compareTo(other.writerId);
    }

    @Override
    public String toString() {
        return sequence + "@" + writerId;
    }

    public static Version parse(String s) {
        int at = s.indexOf('@');
        return new Version(Long.parseLong(s.substring(0, at)), s.substring(at + 1));
    }
}
