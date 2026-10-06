package com.github.feng111627.quorumreplicastorejava.node;

import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import com.github.feng111627.quorumreplicastorejava.core.VersionedValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 预写日志：写入先追加落盘并 fsync，再进内存；
 * 重启后重放日志恢复到崩溃前已提交状态。
 * 记录格式（二进制）：opId, key, tombstone, value(可空), clock。
 * 尾部半条记录（崩溃时写了一半）会被容忍并截断。
 */
public class Wal implements Closeable {
    private final Path file;
    private final DataOutputStream out;
    private final FileOutputStream fos;

    public Wal(Path file) {
        try {
            this.file = file;
            Files.createDirectories(file.getParent());
            this.fos = new FileOutputStream(file.toFile(), true);
            this.out = new DataOutputStream(new BufferedOutputStream(fos));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 追加一条记录并 fsync。 */
    public synchronized void append(String opId, String key, VersionedValue value) {
        try {
            out.writeUTF(opId);
            out.writeUTF(key);
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

    /** 重放日志：每条记录回调 (key, value)，opId 回调用于幂等去重；容忍尾部半条记录。 */
    public static void replay(Path file, BiConsumer<String, VersionedValue> apply,
                              Consumer<String> seenOp) {
        if (!Files.exists(file)) return;
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file.toFile())))) {
            while (true) {
                String opId;
                try {
                    opId = in.readUTF();
                } catch (EOFException e) {
                    break; // 正常结束或尾部半条记录
                }
                String key = in.readUTF();
                boolean tombstone = in.readBoolean();
                String value = in.readBoolean() ? in.readUTF() : null;
                int n = in.readInt();
                Map<String, Long> entries = new HashMap<>();
                for (int i = 0; i < n; i++) {
                    entries.put(in.readUTF(), in.readLong());
                }
                seenOp.accept(opId);
                apply.accept(key, new VersionedValue(value, new VectorClock(entries), tombstone));
            }
        } catch (EOFException e) {
            // 尾部半条记录：忽略，已提交的前缀不受影响
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
