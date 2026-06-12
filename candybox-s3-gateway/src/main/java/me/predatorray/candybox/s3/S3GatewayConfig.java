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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import me.predatorray.candybox.common.config.SecurityConfig;

/**
 * Deployment-facing configuration for the S3 gateway process, loaded from a {@code .properties} file
 * with environment-variable overrides — the same 12-factor convention as the storage node's
 * {@code ServerConfig}: each key maps to {@code CANDYBOX_<KEY>} (dots to underscores, upper-cased) and
 * the environment wins. See {@code S3_GATEWAY_PLAN.md} §11.
 */
public final class S3GatewayConfig {

    /** Default S3 HTTP listen port (plain HTTP; TLS is terminated at the load balancer). */
    public static final int DEFAULT_PORT = 9711;
    /** Default HTTP health/metrics port. */
    public static final int DEFAULT_HEALTH_PORT = 9712;
    /** Default single-PUT ceiling (no multipart in v1): 5 GiB. */
    public static final long DEFAULT_MAX_OBJECT_BYTES = 5L * 1024 * 1024 * 1024;

    private static final String ENV_PREFIX = "CANDYBOX_";

    private final String bindHost;
    private final int bindPort;
    private final int healthPort;
    private final String zookeeperConnect;
    private final String region;
    private final long maxObjectBytes;
    private final int workerThreads;
    private final long routerCacheTtlMillis;
    private final SecurityConfig security;

    private S3GatewayConfig(Builder b) {
        this.bindHost = b.bindHost;
        this.bindPort = b.bindPort;
        this.healthPort = b.healthPort;
        this.zookeeperConnect = b.zookeeperConnect;
        this.region = b.region;
        this.maxObjectBytes = b.maxObjectBytes;
        this.workerThreads = b.workerThreads;
        this.routerCacheTtlMillis = b.routerCacheTtlMillis;
        this.security = b.security;
    }

    public static S3GatewayConfig load(Path propertiesFile) {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(propertiesFile)) {
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read config file: " + propertiesFile, e);
        }
        return fromProperties(props, System.getenv());
    }

    public static S3GatewayConfig fromProperties(Properties props, Map<String, String> env) {
        Resolver r = new Resolver(props, env);
        HostPort bind = HostPort.parse(r.get("s3.bind").orElse("0.0.0.0:" + DEFAULT_PORT), DEFAULT_PORT);
        return new Builder()
                .bindHost(bind.host)
                .bindPort(bind.port)
                .healthPort(r.getInt("health.port").orElse(DEFAULT_HEALTH_PORT))
                .zookeeperConnect(r.require("zookeeper.connect"))
                .region(r.get("s3.region").orElse("us-east-1"))
                .maxObjectBytes(r.getLong("s3.max-object-bytes").orElse(DEFAULT_MAX_OBJECT_BYTES))
                .workerThreads(r.getInt("s3.worker-threads")
                        .orElse(Math.max(4, Runtime.getRuntime().availableProcessors() * 2)))
                .routerCacheTtlMillis(r.getLong("s3.router-cache-ttl-ms").orElse(5_000L))
                .security(SecurityConfig.resolve(r::get))
                .build();
    }

    public String bindHost() {
        return bindHost;
    }

    public int bindPort() {
        return bindPort;
    }

    public int healthPort() {
        return healthPort;
    }

    public String zookeeperConnect() {
        return zookeeperConnect;
    }

    public String region() {
        return region;
    }

    public long maxObjectBytes() {
        return maxObjectBytes;
    }

    public int workerThreads() {
        return workerThreads;
    }

    public long routerCacheTtlMillis() {
        return routerCacheTtlMillis;
    }

    /** The shared {@code auth.*} / {@code tls.*} surface: how this gateway dials the nodes. */
    public SecurityConfig security() {
        return security;
    }

    private record HostPort(String host, int port) {
        static HostPort parse(String value, int defaultPort) {
            int colon = value.lastIndexOf(':');
            if (colon < 0) {
                return new HostPort(value, defaultPort);
            }
            String host = value.substring(0, colon);
            int port = Integer.parseInt(value.substring(colon + 1).trim());
            return new HostPort(host.isEmpty() ? "0.0.0.0" : host, port);
        }
    }

    private static final class Resolver {
        private final Properties props;
        private final Map<String, String> env;

        Resolver(Properties props, Map<String, String> env) {
            this.props = props;
            this.env = env;
        }

        Optional<String> get(String key) {
            String envVal = env.get(ENV_PREFIX + key.toUpperCase().replace('.', '_').replace('-', '_'));
            if (envVal != null && !envVal.isBlank()) {
                return Optional.of(envVal.trim());
            }
            String fileVal = props.getProperty(key);
            return (fileVal != null && !fileVal.isBlank()) ? Optional.of(fileVal.trim()) : Optional.empty();
        }

        String require(String key) {
            return get(key).orElseThrow(() -> new IllegalArgumentException(
                    "Missing required config key '" + key + "' (or env "
                            + ENV_PREFIX + key.toUpperCase().replace('.', '_').replace('-', '_') + ")"));
        }

        Optional<Integer> getInt(String key) {
            return get(key).map(Integer::parseInt);
        }

        Optional<Long> getLong(String key) {
            return get(key).map(Long::parseLong);
        }
    }

    static final class Builder {
        private String bindHost = "0.0.0.0";
        private int bindPort = DEFAULT_PORT;
        private int healthPort = DEFAULT_HEALTH_PORT;
        private String zookeeperConnect;
        private String region = "us-east-1";
        private long maxObjectBytes = DEFAULT_MAX_OBJECT_BYTES;
        private SecurityConfig security = SecurityConfig.disabled();
        private int workerThreads = 8;
        private long routerCacheTtlMillis = 5_000L;

        Builder bindHost(String v) {
            this.bindHost = v;
            return this;
        }

        Builder bindPort(int v) {
            this.bindPort = v;
            return this;
        }

        Builder healthPort(int v) {
            this.healthPort = v;
            return this;
        }

        Builder zookeeperConnect(String v) {
            this.zookeeperConnect = v;
            return this;
        }

        Builder region(String v) {
            this.region = v;
            return this;
        }

        Builder maxObjectBytes(long v) {
            this.maxObjectBytes = v;
            return this;
        }

        Builder workerThreads(int v) {
            this.workerThreads = v;
            return this;
        }

        Builder routerCacheTtlMillis(long v) {
            this.routerCacheTtlMillis = v;
            return this;
        }

        Builder security(SecurityConfig v) {
            this.security = v;
            return this;
        }

        S3GatewayConfig build() {
            return new S3GatewayConfig(this);
        }
    }
}
