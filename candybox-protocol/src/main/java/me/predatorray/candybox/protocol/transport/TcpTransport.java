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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.ProtocolException;

/**
 * A blocking TCP {@link Transport}. Each {@link #connect} opens a socket; calls on a connection are
 * serialized (one in-flight request at a time). With an {@link SSLContext} every connection speaks
 * TLS, verifying the server certificate against the context's trust and — unless disabled for dev
 * certificates — that its SAN matches the host being dialed (HTTPS-style endpoint identification).
 *
 * <p>TODO(phase-2): connection pooling, async pipelining, retry/routing on top of this.
 */
public final class TcpTransport implements Transport {

    private final FrameCodec codec;
    private final SSLContext sslContext;
    private final boolean verifyEndpoint;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;

    /** Default time to establish a connection (including the TLS handshake). */
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5_000;
    /** Default time to wait for a response frame; must exceed the slowest legitimate server op. */
    public static final int DEFAULT_READ_TIMEOUT_MILLIS = 120_000;

    public TcpTransport() {
        this(new FrameCodec());
    }

    public TcpTransport(FrameCodec codec) {
        this(codec, null, true);
    }

    /**
     * @param sslContext     the client TLS context (trust + optional mTLS key), or null for plaintext
     * @param verifyEndpoint verify the server certificate's SAN against the dialed host; disable
     *                       only for dev certificates without proper SANs
     */
    public TcpTransport(FrameCodec codec, SSLContext sslContext, boolean verifyEndpoint) {
        this(codec, sslContext, verifyEndpoint, DEFAULT_CONNECT_TIMEOUT_MILLIS,
                DEFAULT_READ_TIMEOUT_MILLIS);
    }

    /**
     * @param connectTimeoutMillis bound on TCP connect plus TLS handshake; 0 means no limit
     * @param readTimeoutMillis    bound on waiting for a response; on expiry the call fails and the
     *                             connection must be discarded. 0 means no limit
     */
    public TcpTransport(FrameCodec codec, SSLContext sslContext, boolean verifyEndpoint,
            int connectTimeoutMillis, int readTimeoutMillis) {
        if (connectTimeoutMillis < 0 || readTimeoutMillis < 0) {
            throw new IllegalArgumentException("timeouts must be >= 0");
        }
        this.codec = codec;
        this.sslContext = sslContext;
        this.verifyEndpoint = verifyEndpoint;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    @Override
    public Connection connect(String host, int port) {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            if (sslContext != null) {
                socket.setSoTimeout(connectTimeoutMillis);
                SSLSocket ssl = (SSLSocket) sslContext.getSocketFactory()
                        .createSocket(socket, host, port, true);
                if (verifyEndpoint) {
                    SSLParameters params = ssl.getSSLParameters();
                    params.setEndpointIdentificationAlgorithm("HTTPS");
                    ssl.setSSLParameters(params);
                }
                ssl.startHandshake();
                socket = ssl;
            }
            socket.setSoTimeout(readTimeoutMillis);
            return new TcpConnection(socket, codec);
        } catch (IOException e) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // best effort
            }
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
