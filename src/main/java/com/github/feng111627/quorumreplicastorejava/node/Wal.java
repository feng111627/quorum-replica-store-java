package com.github.feng111627.quorumreplicastorejava.node;

import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * 预写日志（v2）：
 * 新文件以 4 字节魔数 {@code QRS1} 开头，随后是带类型的记录：
 * <pre>
 *   PREPARE(1)  opId, key, tombstone, hasValue, value?, clock   —— 两阶段写的预备值
 *   COMMIT (2)  key, clock                                     —— 本副本收到提交决定
 *   ABORT  (3)  key, clock                                     —— 本副本收到放弃决定
 *   DECIDE (4)  commitFlag, key, clock                          —— 协调器全局提交/放弃决定
 * </pre>
 * 每条记录追加后 fsync；尾部半条记录（崩溃时写了一半）会被容忍并停在该处。
 * 没有魔数的旧格式日志仍可读取：旧记录一律按已提交数据回放，保证旧 WAL 可恢复。
 */
public class Wal implements Closeable {

        /** v2 记录类型：预备值 / 提交决定 / 放弃决定 / 放弃残留已物理回收 / 协调器全局决定；0 仅用于旧格式回放回调。 */
    public static final byte TYPE_LEGACY_COMMITTED = 0;
    public static final byte TYPE_PREPARE = 1;
    public static final byte TYPE_COMMIT = 2;
    public static final byte TYPE_ABORT = 3;
    public static final byte TYPE_DECISION = 4;
    public static final byte TYPE_GC_ABORT = 5;

    static final byte[] MAGIC = new byte[] {'Q', 'R', 'S', '1'};

    private final Path file;
    private final DataOutputStream out;
    private final FileOutputStream fos;

    public Wal(Path file) {
        try {
            this.file = file;
            Files.createDirectories(file.getParent());
            boolean fresh = !Files.exists(file) || Files.size(file) == 0;
            this.fos = new FileOutputStream(file.toFile(), true);
            this.out = new DataOutputStream(new BufferedOutputStream(fos));
            if (fresh) writeHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeHeader() throws IOException {
        out.write(MAGIC);
        out.flush();
        fos.getFD().sync();
    }

    // ---- 追加记录 ----

    /** 追加一条预备值（TYPE_PREPARE）并 fsync。 */
    public synchronized void appendPrepare(String opId, String key, VersionedValue value) {
        try {
            out.writeByte(TYPE_PREPARE);
            out.writeUTF(opId);
            out.writeUTF(key);
            writeVersion(out, value);
            flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 追加一条提交决定（TYPE_COMMIT），携带完整版本。
     * 副本持有对应 PREPARE 时用它迁移状态；从未收到 PREPARE（错过阶段一）时也能独立重建已提交版本。
     */
    public synchronized void appendCommit(String key, VersionedValue value) {
        try {
            out.writeByte(TYPE_COMMIT);
            out.writeUTF(key);
            writeVersion(out, value);
            flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 追加一条放弃决定（TYPE_ABORT），只定位到时钟。 */
    public synchronized void appendAbort(String key, VectorClock clock) {
        try {
            out.writeByte(TYPE_ABORT);
            out.writeUTF(key);
            writeClock(out, clock);
            flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 追加一条残留回收记录（TYPE_GC_ABORT）：该时钟的 ABORTED/PENDING 记录已被收敛物理清除。 */
    public synchronized void appendGcAbort(String key, VectorClock clock) {
        try {
            out.writeByte(TYPE_GC_ABORT);
            out.writeUTF(key);
            writeClock(out, clock);
            flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 追加一条协调器全局提交/放弃决定（TYPE_DECISION），提交决定携带 tombstone 以便重启后正确判定删除。 */
    public synchronized void appendGlobalDecision(boolean commit, boolean tombstone,
                                                  String key, VectorClock clock) {
        try {
            out.writeByte(TYPE_DECISION);
            out.writeBoolean(commit);
            out.writeBoolean(tombstone);
            out.writeUTF(key);
            writeClock(out, clock);
            flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void flush() throws IOException {
        out.flush();
        fos.getFD().sync();
    }

    private static void writeVersion(DataOutputStream o, VersionedValue v) throws IOException {
        o.writeBoolean(v.tombstone());
        o.writeBoolean(v.value() != null);
        if (v.value() != null) o.writeUTF(v.value());
        writeClock(o, v.clock());
    }

    private static void writeClock(DataOutputStream o, VectorClock clock) throws IOException {
        Map<String, Long> entries = clock.entries();
        o.writeInt(entries.size());
        for (var e : entries.entrySet()) {
            o.writeUTF(e.getKey());
            o.writeLong(e.getValue());
        }
    }

    private static VersionedValue readVersion(DataInputStream in) throws IOException {
        boolean tombstone = in.readBoolean();
        String value = in.readBoolean() ? in.readUTF() : null;
        VectorClock clock = readClock(in);
        return new VersionedValue(value, clock, tombstone);
    }

    private static VectorClock readClock(DataInputStream in) throws IOException {
        int n = in.readInt();
        Map<String, Long> entries = new HashMap<>();
        for (int i = 0; i < n; i++) {
            entries.put(in.readUTF(), in.readLong());
        }
        return new VectorClock(entries);
    }

    /** v2 记录回调：type 为 TYPE_*；DECISION 的提交标志由 commit 给出，其余恒为 true。 */
    public interface RecordSink {
        void accept(byte type, String opId, boolean commit, String key, VersionedValue value);
    }

    /**
     * 重放日志：自动识别 v2（带魔数）与旧格式。
     * 旧格式记录以 TYPE_PREPARE 后紧跟隐式已提交语义回放，由接收方按 COMMITTED 处理。
     * 尾部半条记录导致的 EOF 被容忍。
     */
    public static void replayV2(Path file, RecordSink sink) {
        if (!Files.exists(file)) return;
        PushbackInputStream pbin = null;
        try {
            pbin = new PushbackInputStream(
                    new BufferedInputStream(new FileInputStream(file.toFile())), MAGIC.length);
            DataInputStream in = new DataInputStream(pbin);
            byte[] head = new byte[MAGIC.length];
            try {
                in.readFully(head);
            } catch (EOFException e) {
                return; // 空文件
            }
            if (java.util.Arrays.equals(head, MAGIC)) {
                replayV2Records(in, sink);
            } else {
                pbin.unread(head); // 旧格式：把首条记录开头读走的 4 字节还回去
                replayLegacyRecords(in, sink);
            }
        } catch (EOFException e) {
            // 尾部半条记录：忽略，已落盘前缀不受影响
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (pbin != null) {
                try { pbin.close(); } catch (IOException ignore) { }
            }
        }
    }

    private static void replayV2Records(DataInputStream in, RecordSink sink) throws IOException {
        while (true) {
            byte type;
            try {
                type = in.readByte();
            } catch (EOFException e) {
                break; // 正常结束
            }
            switch (type) {
                case TYPE_PREPARE -> {
                    String opId = in.readUTF();
                    String key = in.readUTF();
                    VersionedValue v = readVersion(in);
                    sink.accept(TYPE_PREPARE, opId, true, key, v);
                }
                case TYPE_COMMIT -> {
                    String key = in.readUTF();
                    sink.accept(TYPE_COMMIT, null, true, key, readVersion(in));
                }
                case TYPE_ABORT -> {
                    String key = in.readUTF();
                    VectorClock clock = readClock(in);
                    sink.accept(TYPE_ABORT, null, false, key, VersionedValue.of(null, clock));
                }
                case TYPE_DECISION -> {
                    boolean commit = in.readBoolean();
                    boolean tombstone = in.readBoolean();
                    String key = in.readUTF();
                    VectorClock clock = readClock(in);
                    sink.accept(TYPE_DECISION, null, commit, key,
                            new VersionedValue(null, clock, tombstone));
                }
                case TYPE_GC_ABORT -> {
                    String key = in.readUTF();
                    VectorClock clock = readClock(in);
                    sink.accept(TYPE_GC_ABORT, null, false, key, VersionedValue.of(null, clock));
                }
                default -> throw new IOException("unknown wal record type: " + type);
            }
        }
    }

    /** 旧格式记录：opId, key, tombstone, hasValue, value?, clock；全部按已提交数据回放。 */
    private static void replayLegacyRecords(DataInputStream in, RecordSink sink) throws IOException {
        while (true) {
            String opId;
            try {
                opId = in.readUTF();
            } catch (EOFException e) {
                break;
            }
            String key = in.readUTF();
            VersionedValue v = readVersion(in);
            sink.accept(TYPE_LEGACY_COMMITTED, opId, true, key, v);
        }
    }

    @Override
    public synchronized void close() {
        try {
            out.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
