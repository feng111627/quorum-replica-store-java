package com.github.feng111627.quorumreplicastorejava.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** 向量时钟：用于检测并发写冲突与判断版本新旧。 */
public class VectorClock {

    public enum Relation { EQUAL, BEFORE, AFTER, CONCURRENT }

    private final Map<String, Long> entries;

    public VectorClock() {
        this.entries = new HashMap<>();
    }

    public VectorClock(Map<String, Long> entries) {
        this.entries = new HashMap<>(entries);
    }

    public VectorClock copy() {
        return new VectorClock(entries);
    }

    public synchronized void increment(String nodeId) {
        entries.merge(nodeId, 1L, Long::sum);
    }

    public synchronized long get(String nodeId) {
        return entries.getOrDefault(nodeId, 0L);
    }

    public synchronized Map<String, Long> entries() {
        return Map.copyOf(entries);
    }

    /** 合并两个时钟（逐项取最大）。 */
    public static VectorClock merge(VectorClock a, VectorClock b) {
        Map<String, Long> merged = new HashMap<>(a.entries());
        b.entries().forEach((k, v) -> merged.merge(k, v, Math::max));
        return new VectorClock(merged);
    }

    /** 比较两个时钟的偏序关系。 */
    public static Relation compare(VectorClock a, VectorClock b) {
        Map<String, Long> ea = a.entries();
        Map<String, Long> eb = b.entries();
        boolean aGreater = false, bGreater = false;
        var keys = new java.util.HashSet<String>();
        keys.addAll(ea.keySet());
        keys.addAll(eb.keySet());
        for (String k : keys) {
            long va = ea.getOrDefault(k, 0L);
            long vb = eb.getOrDefault(k, 0L);
            if (va > vb) aGreater = true;
            else if (vb > va) bGreater = true;
            if (aGreater && bGreater) return Relation.CONCURRENT;
        }
        if (aGreater) return Relation.AFTER;
        if (bGreater) return Relation.BEFORE;
        return Relation.EQUAL;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VectorClock other)) return false;
        return entries().equals(other.entries());
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries());
    }

    @Override
    public synchronized String toString() {
        return entries.toString();
    }
}
