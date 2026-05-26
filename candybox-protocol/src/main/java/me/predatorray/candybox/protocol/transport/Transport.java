package me.predatorray.candybox.protocol.transport;

/**
 * Client-side transport SPI: opens {@link Connection}s to nodes. Implemented by the real
 * {@link TcpTransport} and the in-JVM {@link LoopbackTransport} used in tests.
 */
public interface Transport extends AutoCloseable {

    /**
     * Opens a connection to a node.
     *
     * @param host node host
     * @param port node port
     * @return a connection
     */
    Connection connect(String host, int port);

    @Override
    void close();
}
