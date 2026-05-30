/*
 * Copyright (c) 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package me.predatorray.candybox.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class S3GatewayConfigTest {

    private static Properties props(String... kv) {
        Properties p = new Properties();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return p;
    }

    @Test
    void appliesDefaultsWhenOnlyTheRequiredKeyIsSet() {
        S3GatewayConfig c = S3GatewayConfig.fromProperties(props("zookeeper.connect", "zk:2181"), Map.of());
        assertThat(c.bindHost()).isEqualTo("0.0.0.0");
        assertThat(c.bindPort()).isEqualTo(S3GatewayConfig.DEFAULT_PORT);
        assertThat(c.healthPort()).isEqualTo(S3GatewayConfig.DEFAULT_HEALTH_PORT);
        assertThat(c.region()).isEqualTo("us-east-1");
        assertThat(c.maxObjectBytes()).isEqualTo(S3GatewayConfig.DEFAULT_MAX_OBJECT_BYTES);
        assertThat(c.routerCacheTtlMillis()).isEqualTo(5_000L);
        assertThat(c.workerThreads()).isGreaterThanOrEqualTo(4);
        assertThat(c.zookeeperConnect()).isEqualTo("zk:2181");
    }

    @Test
    void parsesAllFileKeys() {
        S3GatewayConfig c = S3GatewayConfig.fromProperties(props(
                "zookeeper.connect", "zk:2181",
                "s3.bind", "10.0.0.1:8080",
                "health.port", "8081",
                "s3.region", "eu-west-1",
                "s3.max-object-bytes", "1048576",
                "s3.worker-threads", "16",
                "s3.router-cache-ttl-ms", "250"), Map.of());
        assertThat(c.bindHost()).isEqualTo("10.0.0.1");
        assertThat(c.bindPort()).isEqualTo(8080);
        assertThat(c.healthPort()).isEqualTo(8081);
        assertThat(c.region()).isEqualTo("eu-west-1");
        assertThat(c.maxObjectBytes()).isEqualTo(1_048_576L);
        assertThat(c.workerThreads()).isEqualTo(16);
        assertThat(c.routerCacheTtlMillis()).isEqualTo(250L);
    }

    @Test
    void environmentOverridesFileAndNormalizesDashesAndDots() {
        Properties file = props("zookeeper.connect", "file-zk:2181", "s3.region", "ap-south-1");
        Map<String, String> env = Map.of(
                "CANDYBOX_ZOOKEEPER_CONNECT", "env-zk:2181",
                "CANDYBOX_S3_BIND", "127.0.0.1:9999",
                "CANDYBOX_S3_MAX_OBJECT_BYTES", "42"); // dashes in key -> underscores in env name
        S3GatewayConfig c = S3GatewayConfig.fromProperties(file, env);
        assertThat(c.zookeeperConnect()).isEqualTo("env-zk:2181"); // env wins over file
        assertThat(c.region()).isEqualTo("ap-south-1");            // file value retained
        assertThat(c.bindPort()).isEqualTo(9999);
        assertThat(c.maxObjectBytes()).isEqualTo(42L);
    }

    @Test
    void bindWithoutPortFallsBackToDefaultPort() {
        S3GatewayConfig c = S3GatewayConfig.fromProperties(
                props("zookeeper.connect", "zk:2181", "s3.bind", "myhost"), Map.of());
        assertThat(c.bindHost()).isEqualTo("myhost");
        assertThat(c.bindPort()).isEqualTo(S3GatewayConfig.DEFAULT_PORT);
    }

    @Test
    void blankValuesAreIgnoredAndFallBackToDefaults() {
        S3GatewayConfig c = S3GatewayConfig.fromProperties(
                props("zookeeper.connect", "zk:2181", "s3.region", "  "), Map.of());
        assertThat(c.region()).isEqualTo("us-east-1");
    }

    @Test
    void missingZookeeperConnectIsRejected() {
        assertThatThrownBy(() -> S3GatewayConfig.fromProperties(new Properties(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zookeeper.connect");
    }
}
