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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import me.predatorray.candybox.common.tls.PemTls;

/**
 * The security surface shared by every Candybox process (node, S3 gateway, admin API), parsed from
 * the same {@code auth.*} / {@code tls.*} keys so one mental model covers the fleet. A process uses
 * the parts that apply to it: listeners use the server side ({@code auth.enabled},
 * {@code tls.cert.path}…), and anything dialing a node uses the client side
 * ({@code auth.client.username}…, {@code tls.ca.path}).
 *
 * <pre>
 *   auth.enabled = true                # SASL on the TCP listener
 *   auth.required = true               # false ⇒ unauthenticated connections pass as anonymous
 *   auth.sasl.mechanisms = PLAIN,SCRAM-SHA-256
 *   auth.credentials.file = /etc/candybox/credentials.properties
 *   auth.super.users = Gateway:s3-gw,Admin:ops        # used by the authorizer
 *
 *   auth.client.mechanism = PLAIN      # how this process authenticates to nodes
 *   auth.client.username = node-1
 *   auth.client.password = ...         # or auth.client.password.file (k8s Secret mount)
 *
 *   tls.enabled = true
 *   tls.cert.path = /etc/candybox/tls/tls.crt          # PEM chain (listener / mTLS client cert)
 *   tls.key.path  = /etc/candybox/tls/tls.key          # unencrypted PKCS#8 PEM
 *   tls.ca.path   = /etc/candybox/tls/ca.crt           # trust bundle (client verify / mTLS)
 *   tls.client.auth = false            # listener demands a client certificate (mTLS)
 *   tls.verify.endpoint = true         # client verifies the server SAN matches the dialed host
 * </pre>
 */
public final class SecurityConfig {

    public static final List<String> DEFAULT_MECHANISMS = List.of("PLAIN", "SCRAM-SHA-256");

    private final boolean authEnabled;
    private final boolean authRequired;
    private final List<String> saslMechanisms;
    private final Path credentialsFile;
    private final List<String> superUsers;

    private final String clientMechanism;
    private final String clientUsername;
    private final String clientPassword;

    private final boolean tlsEnabled;
    private final Path tlsCertPath;
    private final Path tlsKeyPath;
    private final Path tlsCaPath;
    private final boolean tlsClientAuth;
    private final boolean tlsVerifyEndpoint;

    private final String zkAuthScheme;
    private final String zkAuthCredentials;
    private final boolean zkAclEnabled;
    private final String metricsAuthToken;

    private SecurityConfig(boolean authEnabled, boolean authRequired, List<String> saslMechanisms,
                           Path credentialsFile, List<String> superUsers, String clientMechanism,
                           String clientUsername, String clientPassword, boolean tlsEnabled,
                           Path tlsCertPath, Path tlsKeyPath, Path tlsCaPath, boolean tlsClientAuth,
                           boolean tlsVerifyEndpoint, String zkAuthScheme, String zkAuthCredentials,
                           boolean zkAclEnabled, String metricsAuthToken) {
        this.authEnabled = authEnabled;
        this.authRequired = authRequired;
        this.saslMechanisms = saslMechanisms;
        this.credentialsFile = credentialsFile;
        this.superUsers = superUsers;
        this.clientMechanism = clientMechanism;
        this.clientUsername = clientUsername;
        this.clientPassword = clientPassword;
        this.tlsEnabled = tlsEnabled;
        this.tlsCertPath = tlsCertPath;
        this.tlsKeyPath = tlsKeyPath;
        this.tlsCaPath = tlsCaPath;
        this.tlsClientAuth = tlsClientAuth;
        this.tlsVerifyEndpoint = tlsVerifyEndpoint;
        this.zkAuthScheme = zkAuthScheme;
        this.zkAuthCredentials = zkAuthCredentials;
        this.zkAclEnabled = zkAclEnabled;
        this.metricsAuthToken = metricsAuthToken;
    }

    /** Everything off — the dev/test default. */
    public static SecurityConfig disabled() {
        return resolve(key -> Optional.empty());
    }

    /**
     * Parses from a key resolver (typically "env var wins over properties file"). Validation is
     * eager: enabling auth without a credentials file, or TLS without cert/key, fails at startup
     * rather than at the first connection.
     */
    public static SecurityConfig resolve(Function<String, Optional<String>> get) {
        boolean authEnabled = bool(get, "auth.enabled", false);
        boolean authRequired = bool(get, "auth.required", true);
        List<String> mechanisms = get.apply("auth.sasl.mechanisms")
                .map(v -> Arrays.stream(v.split(",")).map(String::trim)
                        .filter(s -> !s.isEmpty()).toList())
                .orElse(DEFAULT_MECHANISMS);
        Path credentialsFile = get.apply("auth.credentials.file").map(Path::of).orElse(null);
        List<String> superUsers = get.apply("auth.super.users")
                .map(v -> Arrays.stream(v.split(",")).map(String::trim)
                        .filter(s -> !s.isEmpty()).toList())
                .orElse(List.of());
        if (authEnabled && credentialsFile == null) {
            throw new IllegalArgumentException(
                    "auth.enabled=true requires auth.credentials.file");
        }

        String clientMechanism = get.apply("auth.client.mechanism").orElse("PLAIN");
        String clientUsername = get.apply("auth.client.username").orElse(null);
        String clientPassword = get.apply("auth.client.password").orElseGet(
                () -> get.apply("auth.client.password.file").map(SecurityConfig::readSecret)
                        .orElse(null));
        if (clientUsername != null && clientPassword == null) {
            throw new IllegalArgumentException("auth.client.username is set but neither "
                    + "auth.client.password nor auth.client.password.file is");
        }

        boolean tlsEnabled = bool(get, "tls.enabled", false);
        Path certPath = get.apply("tls.cert.path").map(Path::of).orElse(null);
        Path keyPath = get.apply("tls.key.path").map(Path::of).orElse(null);
        Path caPath = get.apply("tls.ca.path").map(Path::of).orElse(null);
        boolean clientAuth = bool(get, "tls.client.auth", false);
        boolean verifyEndpoint = bool(get, "tls.verify.endpoint", true);

        String zkScheme = get.apply("zookeeper.auth.scheme").orElse(null);
        String zkCredentials = get.apply("zookeeper.auth.credentials").orElseGet(
                () -> get.apply("zookeeper.auth.credentials.file")
                        .map(SecurityConfig::readSecret).orElse(null));
        if (zkScheme != null && zkCredentials == null) {
            throw new IllegalArgumentException("zookeeper.auth.scheme is set but neither "
                    + "zookeeper.auth.credentials nor zookeeper.auth.credentials.file is");
        }
        // ACLs default on as soon as the process has a ZK identity to own the znodes with
        // (digest credentials here, or a JAAS Client section for SASL — flagged explicitly).
        boolean zkAclEnabled = bool(get, "zookeeper.acl.enabled", zkScheme != null);

        String metricsToken = get.apply("metrics.auth.token").orElseGet(
                () -> get.apply("metrics.auth.token.file").map(SecurityConfig::readSecret)
                        .orElse(null));

        return new SecurityConfig(authEnabled, authRequired, mechanisms, credentialsFile,
                superUsers, clientMechanism, clientUsername, clientPassword, tlsEnabled, certPath,
                keyPath, caPath, clientAuth, verifyEndpoint, zkScheme, zkCredentials, zkAclEnabled,
                metricsToken);
    }

    private static boolean bool(Function<String, Optional<String>> get, String key,
                                boolean defaultValue) {
        return get.apply(key).map(Boolean::parseBoolean).orElse(defaultValue);
    }

    private static String readSecret(String file) {
        try {
            return Files.readString(Path.of(file)).trim();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read secret file: " + file, e);
        }
    }

    public boolean authEnabled() {
        return authEnabled;
    }

    public boolean authRequired() {
        return authRequired;
    }

    public List<String> saslMechanisms() {
        return saslMechanisms;
    }

    public Path credentialsFile() {
        return credentialsFile;
    }

    public List<String> superUsers() {
        return superUsers;
    }

    public String clientMechanism() {
        return clientMechanism;
    }

    /** Null when this process has no node-dialing credentials configured. */
    public String clientUsername() {
        return clientUsername;
    }

    public String clientPassword() {
        return clientPassword;
    }

    public boolean tlsEnabled() {
        return tlsEnabled;
    }

    public boolean tlsClientAuth() {
        return tlsClientAuth;
    }

    public boolean tlsVerifyEndpoint() {
        return tlsVerifyEndpoint;
    }

    /** The ZooKeeper {@code addAuth} scheme (e.g. {@code digest}), or null for none/SASL-via-JAAS. */
    public String zkAuthScheme() {
        return zkAuthScheme;
    }

    public String zkAuthCredentials() {
        return zkAuthCredentials;
    }

    public boolean zkAclEnabled() {
        return zkAclEnabled;
    }

    /** When set, node/gateway {@code /metrics} demand {@code Authorization: Bearer <token>}. */
    public String metricsAuthToken() {
        return metricsAuthToken;
    }

    /** The listener-side TLS context, or null when TLS is off. */
    public SSLContext serverSslContext() {
        if (!tlsEnabled) {
            return null;
        }
        if (tlsCertPath == null || tlsKeyPath == null) {
            throw new IllegalArgumentException(
                    "tls.enabled=true requires tls.cert.path and tls.key.path");
        }
        if (tlsClientAuth && tlsCaPath == null) {
            throw new IllegalArgumentException("tls.client.auth=true requires tls.ca.path");
        }
        return PemTls.serverContext(tlsCertPath, tlsKeyPath, tlsCaPath);
    }

    /** The node-dialing TLS context, or null when TLS is off. Presents cert/key when configured
     * (mTLS); trusts {@code tls.ca.path} (else the JVM default trust). */
    public SSLContext clientSslContext() {
        if (!tlsEnabled) {
            return null;
        }
        return PemTls.clientContext(tlsCaPath, tlsCertPath, tlsKeyPath);
    }
}
