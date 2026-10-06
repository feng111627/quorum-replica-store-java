package com.github.feng111627.quorumreplicastorejava.web;

import com.github.feng111627.quorumreplicastorejava.model.OpStatus;
import com.github.feng111627.quorumreplicastorejava.model.ReadResult;
import com.github.feng111627.quorumreplicastorejava.model.Version;
import com.github.feng111627.quorumreplicastorejava.model.WriteResult;
import com.github.feng111627.quorumreplicastorejava.service.CoordinatorService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

/**
 * KV 读写接口。
 * PUT /kv/{key}   body: {"value": "...", "baseVersion": "1@coordinator"(可选), "opId": "..."(可选)}
 * GET /kv/{key}
 * 失败时返回对应状态码与可区分的原因（CONFLICT / WRITE_QUORUM_NOT_MET / READ_QUORUM_NOT_MET）。
 */
@RestController
@RequestMapping("/kv")
public class KvController {

    private final CoordinatorService coordinator;

    public KvController(CoordinatorService coordinator) {
        this.coordinator = coordinator;
    }

    public record WriteRequest(String value, String baseVersion, String opId) {
    }

    @PutMapping("/{key}")
    public ResponseEntity<?> put(@PathVariable String key, @RequestBody WriteRequest req) {
        Version base = req.baseVersion() == null ? null : Version.parse(req.baseVersion());
        String opId = req.opId() == null ? UUID.randomUUID().toString() : req.opId();
        WriteResult result = coordinator.write(key, req.value(), base, opId);
        if (result.status() == OpStatus.OK) {
            return ResponseEntity.ok(Map.of(
                    "status", result.status().name(),
                    "version", result.version().toString()));
        }
        int code = result.status() == OpStatus.CONFLICT ? 409 : 503;
        return ResponseEntity.status(code).body(Map.of(
                "status", result.status().name(),
                "message", result.message()));
    }

    @GetMapping("/{key}")
    public ResponseEntity<?> get(@PathVariable String key) {
        ReadResult result = coordinator.read(key);
        if (result.status() == OpStatus.OK) {
            return ResponseEntity.ok(Map.of(
                    "status", result.status().name(),
                    "value", result.value().value(),
                    "version", result.value().version().toString()));
        }
        int code = switch (result.status()) {
            case KEY_NOT_FOUND -> 404;
            case READ_QUORUM_NOT_MET -> 503;
            default -> 500;
        };
        return ResponseEntity.status(code).body(Map.of(
                "status", result.status().name(),
                "message", result.message()));
    }
}
