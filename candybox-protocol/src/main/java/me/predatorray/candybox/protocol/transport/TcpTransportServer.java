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
package me.predatorray.candybox.protocol.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.ProtocolException;

/**
 * A non-blocking TCP {@link TransportServer}, on Netty NIO: an acceptor event loop, a small pool of
 * I/O event loops that do the framing, and a separate executor group on which the (blocking)
 * {@link RequestHandler} runs, so ledger I/O never stalls an event loop. Connections are no longer
 * paid for in threads — one event loop serves many — so a node's connection count is bounded by
 * file descriptors rather than by stack space.
 *
 * <p>Netty pins each connection to one handler executor for the connection's lifetime, so requests
 * from a connection are handled, and their responses written, in arrival order. That ordering is
 * load-bearing: the wire format carries no request id, so a client matches responses positionally.
 *
 * <p><strong>Pipelining and backpressure.</strong> Further requests are decoded while earlier ones
 * are in flight, bounded per connection by {@link Options#maxInFlightPerConnection}. Reads are also
 * suspended while the channel is unwritable, so a peer that stops draining responses stops the node
 * from accumulating work on its behalf rather than filling the heap with queued output.
 *
 * <p><strong>Draining.</strong> {@link #close()} shuts the listener first so no new connection is
 * accepted, gives in-flight requests up to {@link Options#drainTimeout} to finish and flush their
 * responses, and only then cuts the remaining sockets.
 *
 * <p>With an {@link SSLContext} the listener speaks TLS, optionally demanding a client certificate
 * (mTLS).
 */
public final class TcpTransportServer implements TransportServer {

    /** Slack on top of {@link Options#drainTimeout} when awaiting the handler group's own timeout. */
    private static final long DRAIN_SLACK_MILLIS = 1_000L;
    /** How long {@link #close()} waits for the (by then idle) event loops to stop. */
    private static final long EVENT_LOOP_SHUTDOWN_MILLIS = 2_000L;

    /**
     * Sizing and backpressure knobs.
     *
     * @param ioThreads                event loops doing the framing; 0 means Netty's default
     *                                 (twice the available processors)
     * @param handlerThreads           threads running the blocking {@link RequestHandler}; each
     *                                 connection is pinned to one of them
     * @param maxInFlightPerConnection requests a single connection may have decoded but not yet
     *                                 answered before its reads are suspended
     * @param drainTimeout             how long {@link #close()} lets in-flight requests finish
     */
    public record Options(int ioThreads, int handlerThreads, int maxInFlightPerConnection,
                          Duration drainTimeout) {

        public Options {
            if (ioThreads < 0) {
                throw new IllegalArgumentException("ioThreads must be non-negative");
            }
            if (handlerThreads <= 0) {
                throw new IllegalArgumentException("handlerThreads must be positive");
            }
            if (maxInFlightPerConnection <= 0) {
                throw new IllegalArgumentException("maxInFlightPerConnection must be positive");
            }
            if (drainTimeout == null || drainTimeout.isNegative()) {
                throw new IllegalArgumentException("drainTimeout must be non-negative");
            }
        }

        /**
         * Handler threads sized for work that blocks on BookKeeper rather than on CPU, so the pool
         * is deliberately oversubscribed relative to the core count.
         */
        public static Options defaults() {
            int handlerThreads = Math.max(32, Runtime.getRuntime().availableProcessors() * 4);
            return new Options(0, handlerThreads, 8, Duration.ofSeconds(10));
        }
    }

    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup ioGroup;
    private final EventExecutorGroup handlerGroup;
    private final Set<Channel> connections = ConcurrentHashMap.newKeySet();
    private final Duration drainTimeout;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Channel listener;
    private final int port;

    /** A plaintext listener on all interfaces, with {@link Options#defaults()}. */
    public TcpTransportServer(int port, RequestHandler handler, FrameCodec codec) {
        this(port, handler, codec, null, false);
    }

    /**
     * A listener on all interfaces that speaks TLS when {@code sslContext} is non-null.
     *
     * @param sslContext        the server TLS context (key material loaded), or null for plaintext
     * @param needClientAuth    require a client certificate (mTLS); only meaningful with TLS
     */
    public TcpTransportServer(int port, RequestHandler handler, FrameCodec codec,
                              SSLContext sslContext, boolean needClientAuth) {
        this(null, port, handler, codec, sslContext, needClientAuth, Options.defaults());
    }

    /**
     * The full form.
     *
     * @param bindHost       the interface to bind, or null for all interfaces
     * @param port           the port to bind, 0 for an ephemeral one (see {@link #port()})
     * @param handler        the dispatcher, invoked off the event loop and allowed to block
     * @param codec          supplies the maximum frame size this listener will read or write
     * @param sslContext     the server TLS context, or null for plaintext
     * @param needClientAuth require a client certificate (mTLS); only meaningful with TLS
     * @param options        sizing and backpressure knobs
     */
    public TcpTransportServer(String bindHost, int port, RequestHandler handler, FrameCodec codec,
                              SSLContext sslContext, boolean needClientAuth, Options options) {
        this.drainTimeout = options.drainTimeout();
        this.acceptGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("candybox-accept", true));
        this.ioGroup = new NioEventLoopGroup(options.ioThreads(),
                new DefaultThreadFactory("candybox-io", true));
        this.handlerGroup = new DefaultEventExecutorGroup(options.handlerThreads(),
                new DefaultThreadFactory("candybox-conn", true));

        int maxFrameBytes = codec.maxFrameBytes();
        int maxInFlight = options.maxInFlightPerConnection();
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(acceptGroup, ioGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        if (sslContext != null) {
                            SSLEngine engine = sslContext.createSSLEngine();
                            engine.setUseClientMode(false);
                            engine.setNeedClientAuth(needClientAuth);
                            ch.pipeline().addLast(new SslHandler(engine));
                        }
                        ReadGate gate = new ReadGate(maxInFlight);
                        ch.pipeline().addLast(new FrameDecoder(maxFrameBytes));
                        ch.pipeline().addLast(new FrameEncoder(maxFrameBytes));
                        ch.pipeline().addLast(gate);
                        // The handler blocks on ledger I/O, so it runs off the event loop. Netty pins
                        // the channel to one executor of the group, which is what keeps this
                        // connection's responses in request order.
                        ch.pipeline().addLast(handlerGroup, new RequestDispatcher(handler, gate));
                        connections.add(ch);
                        ch.closeFuture().addListener(ignored -> connections.remove(ch));
                    }
                });

        InetSocketAddress address = bindHost == null
                ? new InetSocketAddress(port)
                : new InetSocketAddress(bindHost, port);
        ChannelFuture bound = bootstrap.bind(address).awaitUninterruptibly();
        if (!bound.isSuccess()) {
            shutdownGroups(); // a failed bind must not leak the pools we just started
            throw new ProtocolException("Failed to bind server socket on port " + port, bound.cause());
        }
        this.listener = bound.channel();
        // Resolved once: a closed channel no longer reports its local address, and callers log the
        // port on the way down as well as on the way up.
        this.port = ((InetSocketAddress) listener.localAddress()).getPort();
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        long drainMillis = drainTimeout.toMillis();
        // 1. Stop accepting. Connections already established keep serving.
        listener.close().awaitUninterruptibly(drainMillis);
        // 2. Let in-flight requests run to completion and their responses reach the event loop,
        //    which is still alive to flush them.
        handlerGroup.shutdownGracefully(0, drainMillis, TimeUnit.MILLISECONDS)
                .awaitUninterruptibly(drainMillis + DRAIN_SLACK_MILLIS);
        // 3. Only now cut whatever is left.
        for (Channel connection : connections) {
            connection.close().awaitUninterruptibly(EVENT_LOOP_SHUTDOWN_MILLIS);
        }
        shutdownGroups();
    }

    /**
     * Stops all three pools and waits for their threads to go, so that a closed server holds no
     * threads: the node's shutdown hook, and tests that start listeners in a loop, both rely on it.
     * Shutdown is requested on all three before awaiting any, so they stop in parallel.
     */
    private void shutdownGroups() {
        Future<?> handlers = handlerGroup.shutdownGracefully(
                0, EVENT_LOOP_SHUTDOWN_MILLIS, TimeUnit.MILLISECONDS);
        Future<?> io = ioGroup.shutdownGracefully(
                0, EVENT_LOOP_SHUTDOWN_MILLIS, TimeUnit.MILLISECONDS);
        Future<?> accept = acceptGroup.shutdownGracefully(
                0, EVENT_LOOP_SHUTDOWN_MILLIS, TimeUnit.MILLISECONDS);
        handlers.awaitUninterruptibly(EVENT_LOOP_SHUTDOWN_MILLIS);
        io.awaitUninterruptibly(EVENT_LOOP_SHUTDOWN_MILLIS);
        accept.awaitUninterruptibly(EVENT_LOOP_SHUTDOWN_MILLIS);
    }
}
