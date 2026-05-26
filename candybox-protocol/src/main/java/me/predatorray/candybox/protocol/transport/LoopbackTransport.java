package me.predatorray.candybox.protocol.transport;

import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;

/**
 * An in-JVM {@link Transport} that delivers requests straight to a {@link RequestHandler} with no
 * sockets. Requests and responses are still round-tripped through {@link FrameCodec} so tests
 * exercise the real wire encoding (including the max-frame cap) without networking.
 */
public final class LoopbackTransport implements Transport {

    private final RequestHandler handler;
    private final FrameCodec codec;

    public LoopbackTransport(RequestHandler handler) {
        this(handler, new FrameCodec());
    }

    public LoopbackTransport(RequestHandler handler, FrameCodec codec) {
        this.handler = handler;
        this.codec = codec;
    }

    @Override
    public Connection connect(String host, int port) {
        return new LoopbackConnection();
    }

    @Override
    public void close() {
        // nothing to release
    }

    private final class LoopbackConnection implements Connection {
        @Override
        public Frame call(Frame request) {
            Frame onWire = codec.decode(codec.encode(request)); // exercise the codec
            Frame response = handler.handle(onWire);
            return codec.decode(codec.encode(response));
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
