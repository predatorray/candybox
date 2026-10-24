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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.Opcode;
import me.predatorray.candybox.protocol.ProtocolException;
import org.junit.jupiter.api.Test;

class TcpTransportTimeoutTest {

    @Test
    void callFailsPromptlyWhenThePeerNeverAnswers() throws Exception {
        try (ServerSocket silent = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                TcpTransport transport = new TcpTransport(new FrameCodec(), null, true, 1_000, 300);
                Connection conn = transport.connect("127.0.0.1", silent.getLocalPort())) {
            long start = System.nanoTime();
            assertThatThrownBy(() -> conn.call(new Frame(Opcode.GET_CANDY, new byte[0])))
                    .isInstanceOf(ProtocolException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
            // the connection is closed, so a retry on it fails fast instead of reading garbage
            assertThatThrownBy(() -> conn.call(new Frame(Opcode.GET_CANDY, new byte[0])))
                    .isInstanceOf(ProtocolException.class);
        }
    }

    // best effort: some networks refuse 10.255.255.1 at once instead of timing out
    @Test
    void connectToNonRoutableAddressFailsWithinTheTimeout() {
        try (TcpTransport transport = new TcpTransport(new FrameCodec(), null, true, 300, 300)) {
            long start = System.nanoTime();
            assertThatThrownBy(() -> transport.connect("10.255.255.1", 9))
                    .isInstanceOf(ProtocolException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
        }
    }

    @Test
    void invalidPortIsRejected() {
        try (TcpTransport transport = new TcpTransport()) {
            assertThatThrownBy(() -> transport.connect("127.0.0.1", 70_000))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void negativeTimeoutsAreRejected() {
        assertThatThrownBy(() -> new TcpTransport(new FrameCodec(), null, true, -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
