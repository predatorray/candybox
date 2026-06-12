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
package me.predatorray.candybox.s3;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty bootstrap for the S3 gateway: a boss/worker event-loop pair handling HTTP framing, and a
 * dedicated blocking {@link EventExecutorGroup} on which {@link S3Handler} runs so the synchronous
 * Candybox client calls never block an I/O thread.
 *
 * <p>v1 aggregates each request into a {@code FullHttpRequest} (bounded by the configured max object
 * size, itself capped to ~2 GiB by the aggregator's int limit), consistent with the client's current
 * buffer-based API. True streaming awaits client streaming support — see {@code S3_GATEWAY_PLAN.md} §4.
 */
final class S3GatewayServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(S3GatewayServer.class);

    private final S3GatewayConfig config;
    private final CandyStore store;
    private final S3Authenticator authenticator;
    private final S3AccessControl access;

    private EventLoopGroup boss;
    private EventLoopGroup workers;
    private EventExecutorGroup blockingGroup;
    private Channel channel;

    S3GatewayServer(S3GatewayConfig config, CandyStore store) {
        this.config = config;
        this.store = store;
        boolean authEnabled = config.s3AuthEnabled();
        me.predatorray.candybox.common.auth.S3KeyStore keys;
        if (authEnabled) {
            if (config.security().credentialsFile() == null) {
                throw new IllegalArgumentException(
                        "s3.auth.enabled=true requires auth.credentials.file (the s3.key.* entries)");
            }
            keys = new me.predatorray.candybox.common.auth.FileCredentialStore(
                    config.security().credentialsFile());
        } else {
            keys = accessKeyId -> java.util.Optional.empty();
        }
        this.authenticator = new S3Authenticator(authEnabled, config.s3AllowAnonymous(), keys,
                config.region(), me.predatorray.candybox.common.SystemClock.INSTANCE);
        this.access = new S3AccessControl(authEnabled, store);
    }

    void start() {
        int maxContent = (int) Math.min(config.maxObjectBytes(), Integer.MAX_VALUE - 8L);
        boss = new NioEventLoopGroup(1);
        workers = new NioEventLoopGroup();
        blockingGroup = new DefaultEventExecutorGroup(config.workerThreads());
        javax.net.ssl.SSLContext sslContext = config.security().serverSslContext();

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        if (sslContext != null) {
                            javax.net.ssl.SSLEngine engine = sslContext.createSSLEngine();
                            engine.setUseClientMode(false);
                            ch.pipeline().addLast(new io.netty.handler.ssl.SslHandler(engine));
                        }
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(maxContent));
                        // Run the (blocking) handler off the I/O event loop.
                        ch.pipeline().addLast(blockingGroup,
                                new S3Handler(store, config, authenticator, access));
                    }
                });

        channel = bootstrap.bind(new InetSocketAddress(config.bindHost(), config.bindPort()))
                .syncUninterruptibly().channel();
        LOG.info("S3 gateway listening on {}:{}", config.bindHost(), port());
    }

    int port() {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close().syncUninterruptibly();
        }
        if (boss != null) {
            boss.shutdownGracefully();
        }
        if (workers != null) {
            workers.shutdownGracefully();
        }
        if (blockingGroup != null) {
            blockingGroup.shutdownGracefully();
        }
    }
}
