package me.predatorray.candybox.client;

import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;

/**
 * A {@link Router} that talks to a single fixed node over one connection, ignoring Box ownership. Used
 * for single-node deployments and tests; a {@code MOVED} response is passed straight through (the
 * {@link CandyboxClient} surfaces it as a {@code NotOwnerException} rather than re-routing).
 */
final class DirectRouter implements Router {

    private final Connection connection;
    private final MessageCodec codec = new MessageCodec();

    DirectRouter(Transport transport, String host, int port) {
        this.connection = transport.connect(host, port);
    }

    @Override
    public Message callBox(String box, Message request) {
        return send(request);
    }

    @Override
    public Message callAny(Message request) {
        return send(request);
    }

    private Message send(Message request) {
        Frame response = connection.call(codec.encode(request));
        return codec.decode(response);
    }

    @Override
    public void close() {
        connection.close();
    }
}
