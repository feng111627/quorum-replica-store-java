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
}
