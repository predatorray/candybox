package me.predatorray.candybox.protocol.transport;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.ProtocolException;

/**
 * A blocking TCP {@link Transport}. Each {@link #connect} opens a socket; calls on a connection are
 * serialized (one in-flight request at a time).
 *
 * <p>TODO(phase-2): connection pooling, async pipelining, retry/routing on top of this.
 */
public final class TcpTransport implements Transport {

    private final FrameCodec codec;

    public TcpTransport() {
        this(new FrameCodec());
    }

    public TcpTransport(FrameCodec codec) {
        this.codec = codec;
    }

    @Override
    public Connection connect(String host, int port) {
        try {
            return new TcpConnection(new Socket(host, port), codec);
        } catch (IOException e) {
            throw new ProtocolException("Failed to connect to " + host + ":" + port, e);
        }
    }

    @Override
    public void close() {
        // connections are closed individually
    }

    private static final class TcpConnection implements Connection {
        private final Socket socket;
        private final DataInputStream in;
        private final OutputStream out;
        private final FrameCodec codec;

        TcpConnection(Socket socket, FrameCodec codec) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(socket.getInputStream());
            this.out = new BufferedOutputStream(socket.getOutputStream());
            this.codec = codec;
        }

        @Override
        public synchronized Frame call(Frame request) {
            try {
                codec.write(out, request);
                return codec.read(in);
            } catch (IOException e) {
                throw new ProtocolException("RPC failed", e);
            }
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException e) {
                throw new ProtocolException("Failed to close connection", e);
            }
        }
    }
}
