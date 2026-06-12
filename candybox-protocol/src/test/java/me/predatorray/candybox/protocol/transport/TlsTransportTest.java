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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URISyntaxException;
import java.nio.file.Path;
import javax.net.ssl.SSLContext;
import me.predatorray.candybox.common.tls.PemTls;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.Opcode;
import me.predatorray.candybox.protocol.ProtocolException;
import org.junit.jupiter.api.Test;

/**
 * End-to-end TLS over the real sockets: server cert verification (including the failure when the
 * client does not trust the CA) and mTLS (client certificate demanded and verified). The test
 * certificates live in {@code src/test/resources/tls} — a long-lived CA, a server certificate with
 * {@code SAN=localhost,127.0.0.1}, and a client certificate.
 */
class TlsTransportTest {

    private static final FrameCodec CODEC = new FrameCodec();
    private static final RequestHandler ECHO =
            request -> new Frame(Opcode.RESPONSE_OK, request.payload());

    private static Path resource(String name) throws URISyntaxException {
        return Path.of(TlsTransportTest.class.getResource("/tls/" + name).toURI());
    }

    private static SSLContext serverContext() throws URISyntaxException {
        return PemTls.serverContext(resource("server.pem"), resource("server.key"),
                resource("ca.pem"));
    }

    @Test
    void roundTripsOverTls() throws Exception {
        try (TcpTransportServer server =
                     new TcpTransportServer(0, ECHO, CODEC, serverContext(), false)) {
            SSLContext client = PemTls.clientContext(resource("ca.pem"), null, null);
            try (TcpTransport transport = new TcpTransport(CODEC, client, true);
                 Connection connection = transport.connect("localhost", server.port())) {
                byte[] payload = {1, 2, 3};
                Frame response = connection.call(new Frame(Opcode.LIST_BOXES, payload));
                assertArrayEquals(payload, response.payload());
            }
        }
    }

    @Test
    void clientWithoutTheCaRejectsTheServer() throws Exception {
        try (TcpTransportServer server =
                     new TcpTransportServer(0, ECHO, CODEC, serverContext(), false)) {
            // Default JVM trust does not include the test CA.
            SSLContext untrusting = PemTls.clientContext(null, null, null);
            try (TcpTransport transport = new TcpTransport(CODEC, untrusting, true)) {
                assertThrows(ProtocolException.class,
                        () -> transport.connect("localhost", server.port()));
            }
        }
    }

    @Test
    void mtlsAcceptsAClientCertificateSignedByTheCa() throws Exception {
        try (TcpTransportServer server =
                     new TcpTransportServer(0, ECHO, CODEC, serverContext(), true)) {
            SSLContext client = PemTls.clientContext(resource("ca.pem"), resource("client.pem"),
                    resource("client.key"));
            try (TcpTransport transport = new TcpTransport(CODEC, client, true);
                 Connection connection = transport.connect("localhost", server.port())) {
                Frame response = connection.call(new Frame(Opcode.LIST_BOXES, new byte[] {7}));
                assertArrayEquals(new byte[] {7}, response.payload());
            }
        }
    }

    @Test
    void mtlsRejectsAClientWithoutACertificate() throws Exception {
        try (TcpTransportServer server =
                     new TcpTransportServer(0, ECHO, CODEC, serverContext(), true)) {
            SSLContext client = PemTls.clientContext(resource("ca.pem"), null, null);
            try (TcpTransport transport = new TcpTransport(CODEC, client, true)) {
                // The handshake failure may surface at connect or at the first call.
                assertThrows(ProtocolException.class, () -> {
                    try (Connection connection = transport.connect("localhost", server.port())) {
                        connection.call(new Frame(Opcode.LIST_BOXES, new byte[0]));
                    }
                });
            }
        }
    }
}
