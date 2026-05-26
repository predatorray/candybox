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
