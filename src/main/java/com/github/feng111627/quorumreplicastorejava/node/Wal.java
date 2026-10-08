package com.github.feng111627.quorumreplicastorejava.node;

import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * 预写日志（格式 v2）：写入先追加落盘并 fsync，再进内存。
 * 记录类型：
 *   PUT    —— 携带 committed 标志：false=两阶段写的 prepare（未提交），true=已提交合并（读修复/收敛）
 *   COMMIT —— 把某个 prepare 过的操作标记为已提交
 *   ABORT  —— 撤销某个 prepare 过的操作（写未凑够法定人数，当作从未发生）
 * 文件头带魔数与版本号；与 v1（无记录类型）不兼容，重放旧文件会给出明确报错。
 * 尾部半条记录（崩溃时写了一半）会被容忍并忽略。
 */
public class Wal implements Closeable {
    static final int MAGIC = 0x51525357;   // "QRSW"
    static final int VERSION = 2;

    static final byte REC_PUT = 0;
    static final byte REC_COMMIT = 1;
    static final byte REC_ABORT = 2;

    private final Path file;
    private final DataOutputStream out;
    private final FileOutputStream fos;

    public Wal(Path file) {
        try {
            this.file = file;
            Files.createDirectories(file.getParent());
            boolean needHeader = !Files.exists(file) || Files.size(file) == 0;
            this.fos = new FileOutputStream(file.toFile(), true);
            this.out = new DataOutputStream(new BufferedOutputStream(fos));
            if (needHeader) {
                out.writeInt(MAGIC);
                out.writeInt(VERSION);
                out.flush();
                fos.getFD().sync();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 追加一条 PUT 记录（committed=false 表示未提交的 prepare）并 fsync。 */
    public synchronized void appendPut(String opId, String key, VersionedValue value, boolean committed) {
        try {
            out.writeByte(REC_PUT);
            out.writeUTF(opId);
            out.writeUTF(key);
            out.writeBoolean(committed);
            out.writeBoolean(value.tombstone());
            out.writeBoolean(value.value() != null);
            if (value.value() != null) out.writeUTF(value.value());
            Map<String, Long> entries = value.clock().entries();
            out.writeInt(entries.size());
            for (var e : entries.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeLong(e.getValue());
            }
            out.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 追加一条 COMMIT 记录并 fsync。 */
    public synchronized void appendCommit(String opId, String key) {
        appendMarker(REC_COMMIT, opId, key);
    }

    /** 追加一条 ABORT 记录并 fsync。 */
    public synchronized void appendAbort(String opId, String key) {
        appendMarker(REC_ABORT, opId, key);
    }

    private synchronized void appendMarker(byte type, String opId, String key) {
        try {
            out.writeByte(type);
            out.writeUTF(opId);
            out.writeUTF(key);
            out.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 重放回调：按日志顺序逐条投递。 */
    public interface ReplayHandler {
        void onPut(String opId, String key, VersionedValue value, boolean committed);
        void onCommit(String opId, String key);
        void onAbort(String opId, String key);
    }

    /** 重放日志：校验文件头，逐条投递；容忍尾部半条记录。 */
    public static void replay(Path file, ReplayHandler handler) {
        if (!Files.exists(file)) return;
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file.toFile())))) {
            int magic = in.readInt();
            int version = in.readInt();
            if (magic != MAGIC || version != VERSION) {
                throw new IllegalStateException(
                        "WAL 格式不兼容: " + file + " (magic=" + Integer.toHexString(magic)
                                + ", version=" + version + "，期望 v" + VERSION
                                + ")——请清空数据目录后重启，或先完成旧版本数据迁移");
            }
            while (true) {
                byte type;
                try {
                    type = in.readByte();
                } catch (EOFException e) {
                    break; // 正常结束或尾部半条记录
                }
                switch (type) {
                    case REC_PUT -> {
                        String opId = in.readUTF();
                        String key = in.readUTF();
                        boolean committed = in.readBoolean();
                        boolean tombstone = in.readBoolean();
                        String value = in.readBoolean() ? in.readUTF() : null;
                        int n = in.readInt();
                        Map<String, Long> entries = new HashMap<>();
                        for (int i = 0; i < n; i++) {
                            entries.put(in.readUTF(), in.readLong());
                        }
                        handler.onPut(opId, key, new VersionedValue(value, new VectorClock(entries), tombstone), committed);
                    }
                    case REC_COMMIT -> handler.onCommit(in.readUTF(), in.readUTF());
                    case REC_ABORT -> handler.onAbort(in.readUTF(), in.readUTF());
                    default -> throw new IllegalStateException("WAL 记录类型无法识别: " + type + " in " + file);
                }
            }
        } catch (EOFException e) {
            // 尾部半条记录：忽略，已落盘的前缀不受影响
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
