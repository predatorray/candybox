package me.predatorray.candybox.protocol.transport;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.ProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A blocking TCP {@link TransportServer}: one accept loop, one handler thread per connection, each
 * reading framed requests and writing framed responses until the peer closes.
 *
 * <p>TODO(phase-2): replace per-connection threads with NIO/Netty, add request pipelining,
 * backpressure, and graceful shutdown draining. This is correct and sufficient for early wiring/tests.
 */
public final class TcpTransportServer implements TransportServer {

    private static final Logger LOG = LoggerFactory.getLogger(TcpTransportServer.class);

    private final ServerSocket serverSocket;
    private final ExecutorService acceptExecutor;
    private final ExecutorService handlerExecutor;
    private final FrameCodec codec;
    private final RequestHandler handler;
    private volatile boolean running = true;

    public TcpTransportServer(int port, RequestHandler handler, FrameCodec codec) {
        this.handler = handler;
        this.codec = codec;
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new ProtocolException("Failed to bind server socket on port " + port, e);
        }
        this.acceptExecutor = Executors.newSingleThreadExecutor(r -> namedDaemon(r, "candybox-accept"));
        this.handlerExecutor = Executors.newCachedThreadPool(r -> namedDaemon(r, "candybox-conn"));
        acceptExecutor.submit(this::acceptLoop);
    }

    @Override
    public int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                handlerExecutor.submit(() -> serve(socket));
            } catch (IOException e) {
                if (running) {
                    LOG.warn("Accept loop error", e);
                }
                return;
            }
        }
    }

    private void serve(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(socket.getInputStream());
             OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {
            while (running) {
                Frame request;
                try {
                    request = codec.read(in);
                } catch (EOFException | SocketException closed) {
                    return; // peer disconnected
                }
                Frame response = handler.handle(request);
                codec.write(out, response);
            }
        } catch (IOException e) {
            LOG.debug("Connection handler ended", e);
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOG.debug("Error closing server socket", e);
        }
        acceptExecutor.shutdownNow();
        handlerExecutor.shutdownNow();
    }

    private static Thread namedDaemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }
}
