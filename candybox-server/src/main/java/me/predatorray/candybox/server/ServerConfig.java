package me.predatorray.candybox.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import me.predatorray.candybox.common.config.CandyboxConfig;

/**
 * Runtime, deployment-facing configuration for a {@link CandyboxServer} process: endpoints, ports,
 * paths and node identity, plus the LSM tuning surface ({@link CandyboxConfig}). This is the
 * complement to {@link CandyboxConfig}, which holds only engine tuning knobs and nothing about
 * <em>where</em> a node listens or which ZooKeeper/BookKeeper it talks to.
 *
 * <p>Values come from a {@code .properties} file, each key overridable by an environment variable
 * (12-factor): the env name is the key upper-cased with {@code .} replaced by {@code _} and a
 * {@code CANDYBOX_} prefix — e.g. {@code server.bind} ⇐ {@code CANDYBOX_SERVER_BIND}. Environment
 * variables win over the file so the same image can be templated across a cluster.
 *
 * <p>Node identity is resolved for cloud-native (StatefulSet) deployment: {@code node.id} is taken
 * from {@code CANDYBOX_NODE_ID} / {@code node.id}, else the trailing ordinal of {@code HOSTNAME}
 * (so pod {@code candybox-2} becomes node {@code 2}). The advertised address published to membership
 * defaults to {@code CANDYBOX_ADVERTISED} / {@code server.advertised}, else the bind address.
 */
public final class ServerConfig {

    /** Default TCP listen port. */
    public static final int DEFAULT_PORT = 9709;
    /** Default HTTP health/metrics port. */
    public static final int DEFAULT_HEALTH_PORT = 9710;

    private static final String ENV_PREFIX = "CANDYBOX_";
    private static final Pattern TRAILING_ORDINAL = Pattern.compile("(\\d+)$");

    private final int nodeId;
    private final String bindHost;
    private final int bindPort;
    private final String advertisedAddress;
    private final int healthPort;
    private final String zookeeperConnect;
    private final String metadataServiceUri;
    private final String coordinationConnect;
    private final byte[] ledgerPassword;
    private final Path dataDir;
    private final Path logDir;
    private final CandyboxConfig tuning;

    private ServerConfig(Builder b) {
        this.nodeId = b.nodeId;
        this.bindHost = b.bindHost;
        this.bindPort = b.bindPort;
        this.advertisedAddress = b.advertisedAddress;
        this.healthPort = b.healthPort;
        this.zookeeperConnect = b.zookeeperConnect;
        this.metadataServiceUri = b.metadataServiceUri;
        this.coordinationConnect = b.coordinationConnect;
        this.ledgerPassword = b.ledgerPassword;
        this.dataDir = b.dataDir;
        this.logDir = b.logDir;
        this.tuning = b.tuning;
    }

    /** Loads configuration from a properties file, layering environment-variable overrides on top. */
    public static ServerConfig load(Path propertiesFile) {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(propertiesFile)) {
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read config file: " + propertiesFile, e);
        }
        return fromProperties(props, System.getenv());
    }

    /** Builds configuration from already-parsed properties and an environment map (for testing). */
    public static ServerConfig fromProperties(Properties props, java.util.Map<String, String> env) {
        Resolver r = new Resolver(props, env);

        String bind = r.get("server.bind").orElse("0.0.0.0:" + DEFAULT_PORT);
        HostPort bindHp = HostPort.parse(bind, DEFAULT_PORT);

        String zk = r.require("zookeeper.connect");
        // Advertised address: friendly CANDYBOX_ADVERTISED env wins (cloud-native pod DNS), then the
        // server.advertised file key, then the bind address.
        String advertised = Optional.ofNullable(env.get("CANDYBOX_ADVERTISED"))
                .filter(s -> !s.isBlank())
                .map(String::trim)
                .or(() -> r.get("server.advertised"))
                .orElse(bindHp.host() + ":" + bindHp.port());

        return new Builder()
                .nodeId(r.resolveNodeId())
                .bindHost(bindHp.host())
                .bindPort(bindHp.port())
                .advertisedAddress(advertised)
                .healthPort(r.getInt("health.port").orElse(DEFAULT_HEALTH_PORT))
                .zookeeperConnect(zk)
                .metadataServiceUri(r.get("bookkeeper.metadataServiceUri")
                        .orElse("zk://" + zk + "/ledgers"))
                .coordinationConnect(r.get("coordination.connect").orElse(zk))
                .ledgerPassword(r.get("ledger.password").orElse("candybox")
                        .getBytes(StandardCharsets.UTF_8))
                .dataDir(Path.of(r.get("data.dir").orElse("./data")))
                .logDir(Path.of(r.get("log.dir").orElse("./logs")))
                .tuning(r.buildTuning())
                .build();
    }

    public int nodeId() {
        return nodeId;
    }

    public String bindHost() {
        return bindHost;
    }

    public int bindPort() {
        return bindPort;
    }

    public String advertisedAddress() {
        return advertisedAddress;
    }

    public int healthPort() {
        return healthPort;
    }

    public String zookeeperConnect() {
        return zookeeperConnect;
    }

    public String metadataServiceUri() {
        return metadataServiceUri;
    }

    public String coordinationConnect() {
        return coordinationConnect;
    }

    public byte[] ledgerPassword() {
        return ledgerPassword.clone();
    }

    public Path dataDir() {
        return dataDir;
    }

    public Path logDir() {
        return logDir;
    }

    public CandyboxConfig tuning() {
        return tuning;
    }

    /** A {@code host:port} pair. */
    record HostPort(String host, int port) {
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

    /** Reads keys from properties with environment-variable precedence, and resolves derived values. */
    private static final class Resolver {
        private final Properties props;
        private final java.util.Map<String, String> env;

        Resolver(Properties props, java.util.Map<String, String> env) {
            this.props = props;
            this.env = env;
        }

        Optional<String> get(String key) {
            String envName = ENV_PREFIX + key.toUpperCase().replace('.', '_');
            String envVal = env.get(envName);
            if (envVal != null && !envVal.isBlank()) {
                return Optional.of(envVal.trim());
            }
            String fileVal = props.getProperty(key);
            if (fileVal != null && !fileVal.isBlank()) {
                return Optional.of(fileVal.trim());
            }
            return Optional.empty();
        }

        String require(String key) {
            return get(key).orElseThrow(() -> new IllegalArgumentException(
                    "Missing required config key '" + key + "' (or env "
                            + ENV_PREFIX + key.toUpperCase().replace('.', '_') + ")"));
        }

        Optional<Integer> getInt(String key) {
            return get(key).map(Integer::parseInt);
        }

        Optional<Long> getLong(String key) {
            return get(key).map(Long::parseLong);
        }

        /** {@code node.id} ⇒ env/file ⇒ HOSTNAME trailing ordinal ⇒ error. */
        int resolveNodeId() {
            Optional<Integer> explicit = getInt("node.id");
            if (explicit.isPresent()) {
                return explicit.get();
            }
            String hostname = env.getOrDefault("HOSTNAME", "");
            Matcher m = TRAILING_ORDINAL.matcher(hostname);
            if (m.find()) {
                return Integer.parseInt(m.group(1));
            }
            throw new IllegalArgumentException(
                    "Cannot resolve node.id: set 'node.id' / CANDYBOX_NODE_ID, or run with a HOSTNAME "
                            + "ending in a numeric ordinal (e.g. a StatefulSet pod 'candybox-0')");
        }

        /** Maps the optional tuning keys onto {@link CandyboxConfig.Builder}; absent keys keep defaults. */
        CandyboxConfig buildTuning() {
            CandyboxConfig.Builder b = CandyboxConfig.builder();
            applyLong("memtable.flush.threshold.bytes", b::memtableFlushThresholdBytes);
            applyLong("syrup.rollover.bytes", b::syrupRolloverBytes);
            applyLong("ownership.lease.ttl.millis", b::ownershipLeaseTtlMillis);
            applyLong("lease.renew.interval.millis", b::leaseRenewIntervalMillis);
            applyLong("router.cache.ttl.millis", b::routerCacheTtlMillis);
            applyLong("compaction.interval.millis", b::compactionIntervalMillis);
            applyLong("max.clock.skew.millis", b::maxClockSkewMillis);
            applyLong("tombstone.gc.grace.millis", b::tombstoneGcGraceMillis);
            applyLong("ledger.gc.grace.millis", b::ledgerGcGraceMillis);
            applyInt("bloom.bits.per.key", b::bloomBitsPerKey);
            applyInt("max.frame.size.bytes", b::maxFrameSizeBytes);
            applyInt("l0.compaction.trigger", b::l0CompactionTrigger);
            applyInt("l0.stall.threshold", b::l0StallThreshold);
            return b.build();
        }

        private void applyLong(String key, Function<Long, ?> setter) {
            getLong(key).ifPresent(setter::apply);
        }

        private void applyInt(String key, Function<Integer, ?> setter) {
            getInt(key).ifPresent(setter::apply);
        }
    }

    static final class Builder {
        private int nodeId;
        private String bindHost = "0.0.0.0";
        private int bindPort = DEFAULT_PORT;
        private String advertisedAddress;
        private int healthPort = DEFAULT_HEALTH_PORT;
        private String zookeeperConnect;
        private String metadataServiceUri;
        private String coordinationConnect;
        private byte[] ledgerPassword = "candybox".getBytes(StandardCharsets.UTF_8);
        private Path dataDir = Path.of("./data");
        private Path logDir = Path.of("./logs");
        private CandyboxConfig tuning = CandyboxConfig.defaults();

        Builder nodeId(int v) {
            this.nodeId = v;
            return this;
        }

        Builder bindHost(String v) {
            this.bindHost = v;
            return this;
        }

        Builder bindPort(int v) {
            this.bindPort = v;
            return this;
        }

        Builder advertisedAddress(String v) {
            this.advertisedAddress = v;
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

        Builder metadataServiceUri(String v) {
            this.metadataServiceUri = v;
            return this;
        }

        Builder coordinationConnect(String v) {
            this.coordinationConnect = v;
            return this;
        }

        Builder ledgerPassword(byte[] v) {
            this.ledgerPassword = v.clone();
            return this;
        }

        Builder dataDir(Path v) {
            this.dataDir = v;
            return this;
        }

        Builder logDir(Path v) {
            this.logDir = v;
            return this;
        }

        Builder tuning(CandyboxConfig v) {
            this.tuning = v;
            return this;
        }

        ServerConfig build() {
            return new ServerConfig(this);
        }
    }
}
