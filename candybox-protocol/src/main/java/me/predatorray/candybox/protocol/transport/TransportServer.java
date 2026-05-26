package me.predatorray.candybox.protocol.transport;

/** Server-side transport: listens for connections and dispatches frames to a {@link RequestHandler}. */
public interface TransportServer extends AutoCloseable {

    /** The port the server is listening on (useful when bound to an ephemeral port). */
    int port();

    @Override
    void close();
}
