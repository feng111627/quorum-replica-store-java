package com.github.feng111627.quorumreplicastorejava.node;

import com.github.feng111627.quorumreplicastorejava.model.Version;
import com.github.feng111627.quorumreplicastorejava.model.VersionedValue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

/**
 * 追加式写前日志：先落盘再改内存，重启时按序重放。
 * 记录格式（key/value/opId 用 Base64 编码避免分隔符冲突）：
 *   PUT      &lt;key64&gt; &lt;value64&gt; &lt;version&gt; &lt;opId64&gt;
 *   ROLLBACK &lt;key64&gt; &lt;opId64&gt; &lt;hadPrev:0|1&gt; [&lt;value64&gt; &lt;version&gt; &lt;opId64&gt;]
 * ROLLBACK 表示撤销 opId 对应的写入并恢复之前的状态（hadPrev=0 表示之前无此键）。
 */
public class WriteAheadLog implements AutoCloseable {

    /** 重放访问器：prev 为 null 表示回滚前该键不存在。 */
    public interface ReplayVisitor {
        void onPut(String key, VersionedValue value);

        void onRollback(String key, String opId, VersionedValue previous);
    }

    private final Path file;
    private BufferedWriter writer;

    public WriteAheadLog(Path file) {
        this.file = file;
        try {
            Files.createDirectories(file.getParent());
            this.writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("无法打开 WAL 文件: " + file, e);
        }
    }

    public synchronized void appendPut(String key, VersionedValue value) {
        writeLine("PUT " + enc(key) + " " + enc(value.value()) + " "
                + value.version() + " " + enc(value.opId()));
    }

    public synchronized void appendRollback(String key, String opId, VersionedValue previous) {
        if (previous == null) {
            writeLine("ROLLBACK " + enc(key) + " " + enc(opId) + " 0");
        } else {
            writeLine("ROLLBACK " + enc(key) + " " + enc(opId) + " 1 "
                    + enc(previous.value()) + " " + previous.version() + " " + enc(previous.opId()));
        }
    }

    /** 按写入顺序重放所有记录，损坏行跳过。 */
    public void replay(ReplayVisitor visitor) {
        if (!Files.exists(file)) {
            return;
        }
        try (var lines = Files.lines(file, StandardCharsets.UTF_8)) {
            lines.forEach(line -> {
                String[] p = line.split(" ");
                if (p.length == 5 && "PUT".equals(p[0])) {
                    visitor.onPut(dec(p[1]),
                            new VersionedValue(dec(p[2]), Version.parse(p[3]), dec(p[4])));
                } else if (p.length >= 4 && "ROLLBACK".equals(p[0])) {
                    VersionedValue prev = null;
                    if ("1".equals(p[3]) && p.length == 7) {
                        prev = new VersionedValue(dec(p[4]), Version.parse(p[5]), dec(p[6]));
                    }
                    visitor.onRollback(dec(p[1]), dec(p[2]), prev);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 重放失败: " + file, e);
        }
    }

    @Override
    public synchronized void close() {
        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeLine(String line) {
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 写入失败: " + file, e);
        }
    }

    private static String enc(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String dec(String s) {
        return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
    }
}
