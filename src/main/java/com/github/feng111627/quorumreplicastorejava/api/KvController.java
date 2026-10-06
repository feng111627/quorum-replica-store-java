package com.github.feng111627.quorumreplicastorejava.api;

import com.github.feng111627.quorumreplicastorejava.cluster.Coordinator;
import com.github.feng111627.quorumreplicastorejava.core.Results.ReadResult;
import com.github.feng111627.quorumreplicastorejava.core.Results.Status;
import com.github.feng111627.quorumreplicastorejava.core.Results.ValueView;
import com.github.feng111627.quorumreplicastorejava.core.Results.WriteResult;
import com.github.feng111627.quorumreplicastorejava.core.VectorClock;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/** 键值读写接口。context 为客户端持有的向量时钟，用于冲突检测与过期拒绝。 */
@RestController
@RequestMapping("/kv")
public class KvController {
    private final Coordinator coordinator;

    public KvController(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public record WriteRequest(String value, String opId, Map<String, Long> context) {}
    public record DeleteRequest(String opId, Map<String, Long> context) {}
    public record ValueDto(String value, Map<String, Long> clock, boolean tombstone) {}
    public record WriteResponse(Status status, Map<String, Long> context, String message) {}
    public record ReadResponse(Status status, List<ValueDto> values,
                               Map<String, Long> context, String message) {}

    @PutMapping("/{key}")
    public ResponseEntity<WriteResponse> put(@PathVariable String key,
                                             @RequestBody WriteRequest req) {
        String opId = req.opId() != null ? req.opId() : UUID.randomUUID().toString();
        VectorClock ctx = req.context() != null ? new VectorClock(req.context()) : null;
        WriteResult r = coordinator.put(opId, key, req.value(), ctx);
        return toResponse(r);
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<WriteResponse> delete(@PathVariable String key,
                                                @RequestBody(required = false) DeleteRequest req) {
        String opId = req != null && req.opId() != null ? req.opId() : UUID.randomUUID().toString();
        VectorClock ctx = req != null && req.context() != null ? new VectorClock(req.context()) : null;
        WriteResult r = coordinator.delete(opId, key, ctx);
        return toResponse(r);
    }

    @GetMapping("/{key}")
    public ResponseEntity<ReadResponse> get(@PathVariable String key) {
        ReadResult r = coordinator.get(key);
        List<ValueDto> values = r.values().stream()
                .map(KvController::toDto)
                .toList();
        Map<String, Long> ctx = r.context() != null ? r.context().entries() : null;
        ReadResponse body = new ReadResponse(r.status(), values, ctx, r.message());
        return switch (r.status()) {
            case OK, NOT_FOUND -> ResponseEntity.ok(body);
            case CONFLICT -> ResponseEntity.status(409).body(body);
            case READ_QUORUM_NOT_MET -> ResponseEntity.status(503).body(body);
            default -> ResponseEntity.internalServerError().body(body);
        };
    }

    private static ResponseEntity<WriteResponse> toResponse(WriteResult r) {
        Map<String, Long> ctx = r.context() != null ? r.context().entries() : null;
        WriteResponse body = new WriteResponse(r.status(), ctx, r.message());
        return switch (r.status()) {
            case OK -> ResponseEntity.ok(body);
            case STALE_CLOCK -> ResponseEntity.status(409).body(body);
            case WRITE_QUORUM_NOT_MET -> ResponseEntity.status(503).body(body);
            default -> ResponseEntity.internalServerError().body(body);
        };
    }

    private static ValueDto toDto(ValueView v) {
        return new ValueDto(v.value(), v.clock().entries(), v.tombstone());
    }
}
