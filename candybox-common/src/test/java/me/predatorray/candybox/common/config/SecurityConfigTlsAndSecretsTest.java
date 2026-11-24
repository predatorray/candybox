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
package me.predatorray.candybox.common.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SecurityConfigTlsAndSecretsTest {
    @TempDir
    Path directory;

    @Test
    void mutualTlsRequiresTrustBundle() throws Exception {
        SecurityConfig config = config(Map.of("tls.enabled", "true", "tls.client.auth", "true",
                "tls.cert.path", resource("server.pem"), "tls.key.path", resource("server.key")));
        assertThatThrownBy(config::serverSslContext).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("tls.client.auth=true requires tls.ca.path");
    }

    @Test
    void serverContextLoadsConfiguredCertificateAndKey() throws Exception {
        SecurityConfig config = config(Map.of("tls.enabled", "true",
                "tls.cert.path", resource("server.pem"), "tls.key.path", resource("server.key")));
        assertThat(config.serverSslContext()).isNotNull();
    }

    @Test
    void clientContextLoadsConfiguredTrustBundle() throws Exception {
        SecurityConfig config = config(Map.of("tls.enabled", "true", "tls.ca.path", resource("ca.pem")));
        assertThat(config.clientSslContext()).isNotNull();
    }

    @Test
    void zooKeeperSchemeRequiresCredentials() {
        assertThatThrownBy(() -> config(Map.of("zookeeper.auth.scheme", "digest")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("zookeeper.auth.scheme is set but neither zookeeper.auth.credentials "
                        + "nor zookeeper.auth.credentials.file is");
    }

    @Test
    void secretFilesAreTrimmedForEveryCredentialKind() throws Exception {
        Path password = Files.writeString(directory.resolve("password"), "s3cret\n");
        Path token = Files.writeString(directory.resolve("token"), "metrics-token\n");
        Path zk = Files.writeString(directory.resolve("zk"), "node:password\n");
        SecurityConfig config = config(Map.of("auth.client.password.file", password.toString(),
                "metrics.auth.token.file", token.toString(), "zookeeper.auth.scheme", "digest",
                "zookeeper.auth.credentials.file", zk.toString()));
        assertThat(config.clientPassword()).isEqualTo("s3cret");
        assertThat(config.metricsAuthToken()).isEqualTo("metrics-token");
        assertThat(config.zkAuthCredentials()).isEqualTo("node:password");
    }

    @Test
    void missingSecretFileHasOperatorFacingError() {
        Path missing = directory.resolve("missing");
        assertThatThrownBy(() -> config(Map.of("auth.client.password.file", missing.toString())))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to read secret file: " + missing);
    }

    private static SecurityConfig config(Map<String, String> values) {
        return SecurityConfig.resolve(key -> Optional.ofNullable(values.get(key)));
    }

    private static String resource(String name) throws Exception {
        return Path.of(SecurityConfigTlsAndSecretsTest.class.getResource("/tls/" + name).toURI()).toString();
    }
}
