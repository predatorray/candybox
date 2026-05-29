package me.predatorray.candybox.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;
import me.predatorray.candybox.server.HealthServer;
import org.junit.jupiter.api.Test;

class ServerConfigTest {

    private static Properties props(String... kv) {
        Properties p = new Properties();
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return p;
    }

    @Test
    void appliesDocumentedDefaultsAndDerivesEndpoints() {
        ServerConfig cfg = ServerConfig.fromProperties(
                props("zookeeper.connect", "zk1:2181", "node.id", "7"), Map.of());

        assertThat(cfg.nodeId()).isEqualTo(7);
        assertThat(cfg.bindHost()).isEqualTo("0.0.0.0");
        assertThat(cfg.bindPort()).isEqualTo(ServerConfig.DEFAULT_PORT);
        assertThat(cfg.healthPort()).isEqualTo(ServerConfig.DEFAULT_HEALTH_PORT);
        // BK metadata URI and coordination connect default to the shared ZK endpoint.
        assertThat(cfg.metadataServiceUri()).isEqualTo("zk://zk1:2181/ledgers");
        assertThat(cfg.coordinationConnect()).isEqualTo("zk1:2181");
        // Advertised address falls back to the bind address.
        assertThat(cfg.advertisedAddress()).isEqualTo("0.0.0.0:" + ServerConfig.DEFAULT_PORT);
    }

    @Test
    void environmentVariablesOverrideFileValues() {
        ServerConfig cfg = ServerConfig.fromProperties(
                props("zookeeper.connect", "file-zk:2181", "node.id", "1", "server.bind", "0.0.0.0:1111"),
                Map.of("CANDYBOX_SERVER_BIND", "0.0.0.0:9000",
                        "CANDYBOX_ZOOKEEPER_CONNECT", "env-zk:2181",
                        "CANDYBOX_ADVERTISED", "pod-3.candybox:9000"));

        assertThat(cfg.bindPort()).isEqualTo(9000);
        assertThat(cfg.zookeeperConnect()).isEqualTo("env-zk:2181");
        assertThat(cfg.metadataServiceUri()).isEqualTo("zk://env-zk:2181/ledgers");
        assertThat(cfg.advertisedAddress()).isEqualTo("pod-3.candybox:9000");
    }

    @Test
    void resolvesNodeIdFromHostnameOrdinalWhenUnset() {
        ServerConfig cfg = ServerConfig.fromProperties(
                props("zookeeper.connect", "zk:2181"),
                Map.of("HOSTNAME", "candybox-statefulset-12"));

        assertThat(cfg.nodeId()).isEqualTo(12);
    }

    @Test
    void explicitNodeIdEnvBeatsHostnameOrdinal() {
        ServerConfig cfg = ServerConfig.fromProperties(
                props("zookeeper.connect", "zk:2181"),
                Map.of("HOSTNAME", "candybox-9", "CANDYBOX_NODE_ID", "42"));

        assertThat(cfg.nodeId()).isEqualTo(42);
    }

    @Test
    void failsWhenNodeIdCannotBeResolved() {
        assertThatThrownBy(() -> ServerConfig.fromProperties(
                props("zookeeper.connect", "zk:2181"), Map.of("HOSTNAME", "no-ordinal-here")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("node.id");
    }

    @Test
    void failsWhenZookeeperConnectMissing() {
        assertThatThrownBy(() -> ServerConfig.fromProperties(props("node.id", "1"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zookeeper.connect");
    }

    @Test
    void mapsTuningKeysOntoCandyboxConfig() {
        ServerConfig cfg = ServerConfig.fromProperties(
                props("zookeeper.connect", "zk:2181", "node.id", "1",
                        "compaction.interval.millis", "5000",
                        "memtable.flush.threshold.bytes", "1048576"),
                Map.of());

        assertThat(cfg.tuning().compactionIntervalMillis()).isEqualTo(5000L);
        assertThat(cfg.tuning().memtableFlushThresholdBytes()).isEqualTo(1048576L);
    }

    @Test
    void parsesPerRoleQuorumOverrides() {
        ServerConfig cfg = ServerConfig.fromProperties(
                props("zookeeper.connect", "zk:2181", "node.id", "1",
                        "quorum.wal", "1/1/1",
                        "quorum.syrup", "3/2/1"),
                Map.of());

        var wal = cfg.tuning().quorum(me.predatorray.candybox.common.config.LedgerRole.WAL);
        assertThat(wal.ensembleSize()).isEqualTo(1);
        assertThat(wal.writeQuorum()).isEqualTo(1);
        assertThat(wal.ackQuorum()).isEqualTo(1);
        var syrup = cfg.tuning().quorum(me.predatorray.candybox.common.config.LedgerRole.SYRUP);
        assertThat(syrup.ensembleSize()).isEqualTo(3);
        assertThat(syrup.ackQuorum()).isEqualTo(1);
        // Roles left unset keep their documented defaults (SSTABLE 3/2/2).
        var sstable = cfg.tuning().quorum(me.predatorray.candybox.common.config.LedgerRole.SSTABLE);
        assertThat(sstable.ensembleSize()).isEqualTo(3);
    }

    @Test
    void rejectsMalformedQuorumOverride() {
        assertThatThrownBy(() -> ServerConfig.fromProperties(
                props("zookeeper.connect", "zk:2181", "node.id", "1", "quorum.wal", "1/1"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quorum.wal");
    }

    @Test
    void rendersPrometheusMetricsWithBoxAndNodeLabels() {
        String text = HealthServer.renderMetrics(3, Map.of("alpha",
                new me.predatorray.candybox.lsm.engine.BoxEngineStats(5, 1, 9, 2, 0, 1, 0, 0)));

        assertThat(text).contains("# TYPE candybox_puts_total counter");
        assertThat(text).contains("candybox_puts_total{node=\"3\",box=\"alpha\"} 5");
        assertThat(text).contains("candybox_owned_boxes{node=\"3\"} 1");
    }
}
