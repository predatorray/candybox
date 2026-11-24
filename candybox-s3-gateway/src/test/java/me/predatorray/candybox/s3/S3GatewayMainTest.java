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

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class S3GatewayMainTest {
    @Test
    void configFilePrecedenceAndBlankFallbacks() {
        Map<String, String> env = Map.of("CANDYBOX_CONF_DIR", "/etc/candybox");
        assertThat(S3GatewayMain.resolveConfigFile(new String[] {"custom.properties"}, env))
                .isEqualTo(Path.of("custom.properties"));
        for (String[] args : new String[][] {new String[0], {"  "}}) {
            assertThat(S3GatewayMain.resolveConfigFile(args, env))
                    .isEqualTo(Path.of("/etc/candybox/candybox-s3-gateway.properties"));
            assertThat(S3GatewayMain.resolveConfigFile(args, Map.of()))
                    .isEqualTo(Path.of("conf/candybox-s3-gateway.properties"));
            assertThat(S3GatewayMain.resolveConfigFile(args, Map.of("CANDYBOX_CONF_DIR", " ")))
                    .isEqualTo(Path.of("conf/candybox-s3-gateway.properties"));
        }
    }

    @Test
    void loadsPropertiesFile(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("gateway.properties");
        Files.writeString(file, "zookeeper.connect=test-zk:2181\ns3.region=eu-west-1\n");
        S3GatewayConfig config = S3GatewayConfig.load(file);
        assertThat(config.zookeeperConnect()).isEqualTo("test-zk:2181");
        assertThat(config.region()).isEqualTo("eu-west-1");
    }

    @Test
    void missingConfigFileReportsPath(@TempDir Path tmp) {
        Path missing = tmp.resolve("missing.properties");
        assertThatThrownBy(() -> S3GatewayMain.main(new String[] {missing.toString()}))
                .isInstanceOf(UncheckedIOException.class).hasMessageContaining(missing.toString());
    }
}
