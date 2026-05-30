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

import static org.assertj.core.api.Assertions.assertThat;

import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Opcode;
import org.junit.jupiter.api.Test;

class TransportTest {

    /** Echoes the request payload back in an OK response. */
    private static final RequestHandler ECHO =
            req -> new Frame(Opcode.RESPONSE_OK, req.payload());

    @Test
    void loopbackTransportDeliversThroughTheCodec() {
        try (LoopbackTransport transport = new LoopbackTransport(ECHO);
             Connection conn = transport.connect("ignored", 0)) {
            Frame response = conn.call(new Frame(Opcode.GET_CANDY, "ping".getBytes()));
            assertThat(response.opcode()).isEqualTo(Opcode.RESPONSE_OK);
            assertThat(new String(response.payload())).isEqualTo("ping");
        }
    }

    @Test
    void tcpTransportRoundTripsOverASocket() {
        try (TcpTransportServer server = new TcpTransportServer(0, ECHO,
                new me.predatorray.candybox.protocol.FrameCodec());
             TcpTransport transport = new TcpTransport()) {
            try (Connection conn = transport.connect("127.0.0.1", server.port())) {
                Frame r1 = conn.call(new Frame(Opcode.GET_CANDY, "one".getBytes()));
                Frame r2 = conn.call(new Frame(Opcode.GET_CANDY, "two".getBytes()));
                assertThat(new String(r1.payload())).isEqualTo("one");
                assertThat(new String(r2.payload())).isEqualTo("two");
            }
        }
    }
}
