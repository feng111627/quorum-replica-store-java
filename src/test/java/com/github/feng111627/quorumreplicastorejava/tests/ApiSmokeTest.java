package com.github.feng111627.quorumreplicastorejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 业务场景：REST 接口端到端冒烟（含 admin 手动上下线节点）。
 * 使用独立临时数据目录，避免污染本地数据。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "cluster.data-dir=${java.io.tmpdir}/kv-smoke-${random.uuid}",
        "cluster.nodes=a1,a2,a3,a4,a5",
        "cluster.shard-count=3",
        "cluster.replicas-per-shard=3",
        "cluster.write-quorum=2",
        "cluster.read-quorum=2"
})
class ApiSmokeTest {
    private static final Logger log = LoggerFactory.getLogger(ApiSmokeTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate http;

    String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    @SuppressWarnings("unchecked")
    void putGetAndAdminDownUp() {
        // 写入
        ResponseEntity<Map> w = http.exchange(url("/kv/hello"), HttpMethod.PUT,
                new HttpEntity<>(Map.of("value", "world", "opId", "smoke-1")), Map.class);
        log.info("[业务:REST冒烟][用例:写入] status={} body={}", w.getStatusCode(), w.getBody());
        assertEquals(HttpStatus.OK, w.getStatusCode());
        assertEquals("OK", w.getBody().get("status"));
        Map<String, Long> context = (Map<String, Long>) w.getBody().get("context");
        assertNotNull(context);

        // 读取
        ResponseEntity<Map> g = http.getForEntity(url("/kv/hello"), Map.class);
        log.info("[业务:REST冒烟][用例:读取] status={} body={}", g.getStatusCode(), g.getBody());
        assertEquals(HttpStatus.OK, g.getStatusCode());
        assertEquals("OK", g.getBody().get("status"));

        // admin：查看节点状态
        ResponseEntity<Map> nodes = http.getForEntity(url("/admin/nodes"), Map.class);
        log.info("[业务:REST冒烟][用例:节点状态] body={}", nodes.getBody());
        assertEquals(5, nodes.getBody().size());

        // admin：手动下线 2 个副本使写法定人数不足
        http.postForEntity(url("/admin/nodes/a1/down"), null, Map.class);
        http.postForEntity(url("/admin/nodes/a2/down"), null, Map.class);
        ResponseEntity<Map> w2 = http.exchange(url("/kv/k-on-a1-shard"), HttpMethod.PUT,
                new HttpEntity<>(Map.of("value", "x", "opId", "smoke-2")), Map.class);
        log.info("[业务:REST冒烟][用例:下线后写入] status={} body={}", w2.getStatusCode(), w2.getBody());
        // a1/a2 下线后，落在其分片的键可能写不足法定人数（503），也可能落在其他分片成功（200）
        assertTrue(w2.getStatusCode() == HttpStatus.OK || w2.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE);

        // admin：恢复节点
        ResponseEntity<Map> up = http.postForEntity(url("/admin/nodes/a1/up"), null, Map.class);
        log.info("[业务:REST冒烟][用例:恢复节点] body={}", up.getBody());
        assertEquals(Boolean.TRUE, up.getBody().get("up"));
        ResponseEntity<Map> nodes2 = http.getForEntity(url("/admin/nodes"), Map.class);
        log.info("[业务:REST冒烟][用例:恢复后节点状态] body={}", nodes2.getBody());
        assertEquals(Boolean.TRUE, nodes2.getBody().get("a1"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void inspectAndRepairEndpoints() {
        String key = "inspect-http-key";
        // 同一上下文可能被前一用例下线过节点，先全部恢复
        for (String n : new String[] {"a1", "a2", "a3", "a4", "a5"}) {
            http.postForEntity(url("/admin/nodes/" + n + "/up"), null, Map.class);
        }
        // 先写一个已提交值，并读一次触发读修复把第三个副本补齐
        ResponseEntity<Map> w = http.exchange(url("/kv/" + key), HttpMethod.PUT,
                new HttpEntity<>(Map.of("value", "v1", "opId", "insp-1")), Map.class);
        assertEquals(HttpStatus.OK, w.getStatusCode());
        assertEquals(HttpStatus.OK, http.getForEntity(url("/kv/" + key), Map.class).getStatusCode());

        // 只读巡检：单键
        ResponseEntity<Map> one = http.getForEntity(url("/admin/inspect/keys/" + key), Map.class);
        log.info("[业务:REST冒烟][用例:巡检单键] body={}", one.getBody());
        assertEquals(HttpStatus.OK, one.getStatusCode());
        assertEquals(Boolean.TRUE, one.getBody().get("consistent"));

        // 只读巡检：整库
        ResponseEntity<Map> all = http.getForEntity(url("/admin/inspect"), Map.class);
        log.info("[业务:REST冒烟][用例:巡检整库] keyCount={} consistent={}",
                all.getBody().get("keyCount"), all.getBody().get("consistent"));
        assertEquals(HttpStatus.OK, all.getStatusCode());

        // 按需收敛：单键 + 整库，重复触发
        ResponseEntity<Map> r1 = http.postForEntity(url("/admin/repair/keys/" + key), null, Map.class);
        ResponseEntity<Map> r2 = http.postForEntity(url("/admin/repair/keys/" + key), null, Map.class);
        ResponseEntity<java.util.List> rall = http.postForEntity(url("/admin/repair"), null, java.util.List.class);
        log.info("[业务:REST冒烟][用例:按需收敛] firstChanged={} secondChanged={} 整库条目数={}",
                r1.getBody().get("changed"), r2.getBody().get("changed"), rall.getBody().size());
        assertEquals(HttpStatus.OK, r1.getStatusCode());
        assertEquals(Boolean.FALSE, r2.getBody().get("changed"), "重复收敛应当幂等无改动");

        // 本地版本视图带上提交状态（从巡检结果取一个确实持有该键的副本）
        java.util.List<Map<String, Object>> reps =
                (java.util.List<Map<String, Object>>) one.getBody().get("replicas");
        Map<String, Object> holder = reps.stream()
                .filter(x -> !((java.util.List<?>) x.get("versions")).isEmpty())
                .findFirst().orElseThrow();
        String replicaId = (String) holder.get("nodeId");
        ResponseEntity<java.util.List> local =
                http.getForEntity(url("/admin/nodes/" + replicaId + "/keys/" + key), java.util.List.class);
        log.info("[业务:REST冒烟][用例:本地版本视图] 副本={} body={}", replicaId, local.getBody());
        assertEquals(HttpStatus.OK, local.getStatusCode());
        Map<String, Object> first = (Map<String, Object>) local.getBody().get(0);
        assertEquals("COMMITTED", first.get("state"));
    }
}
