package me.predatorray.candybox.it;

import java.io.IOException;
import java.net.ServerSocket;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;

/**
 * Boots an in-JVM BookKeeper cluster (with its own embedded ZooKeeper) via {@link LocalBookKeeper},
 * for the integration tests. No external services or Docker; everything runs in the test JVM and is
 * shut down by {@link #close()}.
 */
final class EmbeddedBookKeeper implements AutoCloseable {

    private final int zkPort;
    private final LocalBookKeeper localBookKeeper;

    EmbeddedBookKeeper(int numBookies) {
        this.zkPort = freePort();
        try {
            ServerConfiguration serverConf = new ServerConfiguration();
            serverConf.setMetadataServiceUri(metadataServiceUri());
            serverConf.setAllowLoopback(true);
            // getLocalBookies(zkHost, zkPort, numBookies, shouldStartZK, conf) stands up the in-JVM
            // ZooKeeper and the bookies; start() launches them.
            this.localBookKeeper =
                    LocalBookKeeper.getLocalBookies("127.0.0.1", zkPort, numBookies, true, serverConf);
            this.localBookKeeper.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start embedded BookKeeper", e);
        }
        awaitReady(numBookies);
    }

    private String metadataServiceUri() {
        return "zk://127.0.0.1:" + zkPort + "/ledgers";
    }

    /** A client configuration pointing at the embedded cluster's metadata service. */
    ClientConfiguration clientConfiguration() {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri());
        return conf;
    }

    private void awaitReady(int numBookies) {
        long deadline = System.currentTimeMillis() + 60_000;
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try (BookKeeper bk = new BookKeeper(clientConfiguration())) {
                long id = bk.createLedger(numBookies, numBookies, numBookies,
                        BookKeeper.DigestType.CRC32C, "probe".getBytes()).getId();
                bk.deleteLedger(id);
                return;
            } catch (Exception e) {
                last = new RuntimeException(e);
                sleep(500);
            }
        }
        throw new IllegalStateException("Embedded BookKeeper did not become ready in time", last);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port available", e);
        }
    }

    @Override
    public void close() {
        try {
            localBookKeeper.close();
        } catch (Exception e) {
            // best-effort teardown
        }
    }
}
