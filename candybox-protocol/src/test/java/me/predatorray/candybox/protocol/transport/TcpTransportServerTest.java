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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.Opcode;
import me.predatorray.candybox.protocol.ProtocolException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The NIO listener over real sockets: that connections cost file descriptors rather than threads,
 * that a connection's responses come back in request order however deeply the client pipelines,
 * that backpressure never wedges a connection, that {@link TcpTransportServer#close()} drains
 * in-flight work, and that a malformed frame costs only the connection that sent it.
 */
@Timeout(60)
class TcpTransportServerTest {

    private static final FrameCodec CODEC = new FrameCodec();
    private static final RequestHandler ECHO =
            request -> new Frame(Opcode.RESPONSE_OK, request.payload());

    private static TcpTransportServer server(RequestHandler handler,
                                             TcpTransportServer.Options options) {
        return new TcpTransportServer(null, 0, handler, CODEC, null, false, options);
    }

    /** Opens a raw socket so the tests can write frames without waiting for the previous answer. */
    private static Socket connect(TcpTransportServer server) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 5_000);
        socket.setSoTimeout(30_000);
        return socket;
    }

    private static int liveThreads(String namePrefix) {
        return (int) Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith(namePrefix))
                .count();
    }

    /**
     * The point of the exercise: 40 simultaneous connections served by a 4-thread handler pool and
     * 2 event loops. The blocking server this replaced would have parked one thread per connection
     * for as long as each stayed open.
     */
    @Test
    void servesFarMoreConnectionsThanItHasThreads() throws Exception {
        TcpTransportServer.Options options =
                new TcpTransportServer.Options(2, 4, 8, Duration.ofSeconds(5));
        int connectionCount = 40;
        try (TcpTransportServer server = server(ECHO, options);
             TcpTransport transport = new TcpTransport(CODEC)) {
            int handlerThreadsBefore = liveThreads("candybox-conn");
            int ioThreadsBefore = liveThreads("candybox-io");

            List<Connection> connections = new ArrayList<>();
            ExecutorService clients = Executors.newFixedThreadPool(16);
            try {
                for (int i = 0; i < connectionCount; i++) {
                    connections.add(transport.connect("127.0.0.1", server.port()));
                }
                List<Future<String>> answers = new ArrayList<>();
                for (int i = 0; i < connectionCount; i++) {
                    Connection connection = connections.get(i);
                    String payload = "connection-" + i;
                    answers.add(clients.submit(() -> new String(
                            connection.call(new Frame(Opcode.GET_CANDY, payload.getBytes())).payload(),
                            StandardCharsets.UTF_8)));
                }
                for (int i = 0; i < connectionCount; i++) {
                    assertThat(answers.get(i).get(30, TimeUnit.SECONDS))
                            .isEqualTo("connection-" + i);
                }

                // Still all open at this point, so the thread counts below are the cost of serving
                // 40 live connections.
                assertThat(liveThreads("candybox-conn") - handlerThreadsBefore)
                        .isLessThanOrEqualTo(options.handlerThreads());
                assertThat(liveThreads("candybox-io") - ioThreadsBefore)
                        .isLessThanOrEqualTo(options.ioThreads());
            } finally {
                clients.shutdownNow();
                connections.forEach(Connection::close);
            }
        }
    }

    /**
     * Responses are positional — the frame header carries no request id — so a pipelining client
     * must get them back in the order it asked. The handler stalls the first request to make sure
     * the ordering is enforced rather than incidental.
     */
    @Test
    void answersPipelinedRequestsInOrder() throws Exception {
        RequestHandler slowFirst = request -> {
            if ("0".equals(new String(request.payload(), StandardCharsets.UTF_8))) {
                sleep(300);
            }
            return new Frame(Opcode.RESPONSE_OK, request.payload());
        };
        int requests = 20;
        try (TcpTransportServer server = server(slowFirst, TcpTransportServer.Options.defaults());
             Socket socket = connect(server)) {
            OutputStream out = socket.getOutputStream();
            for (int i = 0; i < requests; i++) {
                out.write(CODEC.encode(new Frame(Opcode.GET_CANDY,
                        String.valueOf(i).getBytes(StandardCharsets.UTF_8))));
            }
            out.flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            for (int i = 0; i < requests; i++) {
                Frame response = CODEC.read(in);
                assertThat(response.opcode()).isEqualTo(Opcode.RESPONSE_OK);
                assertThat(new String(response.payload(), StandardCharsets.UTF_8))
                        .isEqualTo(String.valueOf(i));
            }
        }
    }

    /**
     * Pipelining well past {@code maxInFlightPerConnection} makes the server suspend reads mid-way.
     * Every request must still be answered: a gate that failed to lift would hang here.
     */
    @Test
    void keepsServingWhenTheClientPipelinesPastTheInFlightLimit() throws Exception {
        TcpTransportServer.Options tightGate =
                new TcpTransportServer.Options(1, 2, 2, Duration.ofSeconds(5));
        int requests = 200;
        try (TcpTransportServer server = server(ECHO, tightGate);
             Socket socket = connect(server)) {
            OutputStream out = socket.getOutputStream();
            for (int i = 0; i < requests; i++) {
                out.write(CODEC.encode(new Frame(Opcode.GET_CANDY,
                        String.valueOf(i).getBytes(StandardCharsets.UTF_8))));
            }
            out.flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            for (int i = 0; i < requests; i++) {
                assertThat(new String(CODEC.read(in).payload(), StandardCharsets.UTF_8))
                        .isEqualTo(String.valueOf(i));
            }
        }
    }

    /** A request already running when the server is closed still gets its answer. */
    @Test
    void drainsInFlightRequestsOnClose() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        RequestHandler slow = request -> {
            started.countDown();
            sleep(300);
            return new Frame(Opcode.RESPONSE_OK, request.payload());
        };
        TcpTransportServer server = server(slow,
                new TcpTransportServer.Options(1, 2, 8, Duration.ofSeconds(10)));
        ExecutorService client = Executors.newSingleThreadExecutor();
        try (Socket socket = connect(server)) {
            Future<Frame> answer = client.submit(() -> {
                OutputStream out = socket.getOutputStream();
                out.write(CODEC.encode(new Frame(Opcode.GET_CANDY, "draining".getBytes())));
                out.flush();
                return CODEC.read(new DataInputStream(socket.getInputStream()));
            });
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

            server.close();

            Frame response = answer.get(30, TimeUnit.SECONDS);
            assertThat(response.opcode()).isEqualTo(Opcode.RESPONSE_OK);
            assertThat(new String(response.payload(), StandardCharsets.UTF_8)).isEqualTo("draining");
        } finally {
            client.shutdownNow();
            server.close();
        }
    }

    /** Closing stops the listener: connecting afterwards must fail rather than hang or succeed. */
    @Test
    void stopsAcceptingOnceClosed() throws Exception {
        TcpTransportServer server = server(ECHO, TcpTransportServer.Options.defaults());
        int port = server.port();
        server.close();

        try (Socket socket = new Socket()) {
            assertThatConnectFails(socket, port);
        }
    }

    private static void assertThatConnectFails(Socket socket, int port) {
        try {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 2_000);
            // A connect that somehow succeeds must at least not be served.
            socket.setSoTimeout(2_000);
            socket.getOutputStream().write(CODEC.encode(new Frame(Opcode.GET_CANDY, new byte[0])));
            socket.getOutputStream().flush();
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        } catch (IOException refused) {
            // The expected outcome: nothing is listening any more.
            assertThat(refused).isInstanceOf(IOException.class);
        }
    }

    /** A garbage frame costs the sender its connection and nothing else. */
    @Test
    void aMalformedFrameClosesOnlyTheOffendingConnection() throws Exception {
        AtomicInteger handled = new AtomicInteger();
        RequestHandler counting = request -> {
            handled.incrementAndGet();
            return new Frame(Opcode.RESPONSE_OK, request.payload());
        };
        try (TcpTransportServer server = server(counting, TcpTransportServer.Options.defaults())) {
            try (Socket bad = connect(server)) {
                bad.getOutputStream().write(new byte[] {0x00, 0x01, 0x01, 0x0b, 0, 0, 0, 0});
                bad.getOutputStream().flush();
                assertThat(bad.getInputStream().read()).isEqualTo(-1); // server hung up
            }

            try (TcpTransport transport = new TcpTransport(CODEC);
                 Connection connection = transport.connect("127.0.0.1", server.port())) {
                Frame response = connection.call(new Frame(Opcode.GET_CANDY, "after".getBytes()));
                assertThat(new String(response.payload(), StandardCharsets.UTF_8)).isEqualTo("after");
            }
            assertThat(handled).hasValue(1);
        }
    }

    /**
     * A header claiming 2 GiB is rejected on the header alone: the connection goes without the
     * decoder ever waiting for, or allocating, the payload it advertised.
     */
    @Test
    void rejectsAnOversizedLengthWithoutWaitingForThePayload() throws Exception {
        try (TcpTransportServer server = server(ECHO, TcpTransportServer.Options.defaults());
             Socket socket = connect(server)) {
            OutputStream out = socket.getOutputStream();
            out.write(new byte[] {
                (byte) 0xCB, 0x0F, 0x01, (byte) Opcode.PUT_CANDY.code(),
                0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            });
            out.flush();

            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        }
    }

    /** A handler that fails outright costs the sender its connection, and nothing else. */
    @Test
    void aFailingHandlerClosesOnlyTheOffendingConnection() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RequestHandler failsFirstCall = request -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("handler blew up");
            }
            return new Frame(Opcode.RESPONSE_OK, request.payload());
        };
        try (TcpTransportServer server =
                     server(failsFirstCall, TcpTransportServer.Options.defaults())) {
            try (Socket doomed = connect(server)) {
                doomed.getOutputStream().write(
                        CODEC.encode(new Frame(Opcode.PUT_CANDY, "boom".getBytes())));
                doomed.getOutputStream().flush();
                assertThat(doomed.getInputStream().read()).isEqualTo(-1); // no answer, hung up
            }

            try (TcpTransport transport = new TcpTransport(CODEC);
                 Connection connection = transport.connect("127.0.0.1", server.port())) {
                assertThat(new String(connection.call(
                        new Frame(Opcode.GET_CANDY, "after".getBytes())).payload(),
                        StandardCharsets.UTF_8)).isEqualTo("after");
            }
        }
    }

    /** Binding a port already in use fails loudly — and must not leave its thread pools running. */
    @Test
    void aFailedBindReportsAndLeavesNoThreads() throws Exception {
        try (TcpTransportServer taken = server(ECHO, TcpTransportServer.Options.defaults())) {
            int occupied = taken.port();
            int ioBefore = liveThreads("candybox-io");
            int acceptBefore = liveThreads("candybox-accept");

            assertThatThrownBy(() -> new TcpTransportServer(null, occupied, ECHO, CODEC, null, false,
                    TcpTransportServer.Options.defaults()))
                    .isInstanceOf(ProtocolException.class)
                    .hasMessageContaining("Failed to bind server socket on port " + occupied);

            assertThat(liveThreads("candybox-io")).isEqualTo(ioBefore);
            assertThat(liveThreads("candybox-accept")).isEqualTo(acceptBefore);
        }
    }

    /** The node passes its configured {@code server.bind} host through, rather than every interface. */
    @Test
    void bindsTheRequestedInterface() throws Exception {
        try (TcpTransportServer server = new TcpTransportServer("127.0.0.1", 0, ECHO, CODEC, null,
                false, TcpTransportServer.Options.defaults());
             TcpTransport transport = new TcpTransport(CODEC);
             Connection connection = transport.connect("127.0.0.1", server.port())) {
            assertThat(new String(connection.call(
                    new Frame(Opcode.GET_CANDY, "bound".getBytes())).payload(),
                    StandardCharsets.UTF_8)).isEqualTo("bound");
        }
    }

    /** Closing twice is what a shutdown hook racing an explicit close does; it must be harmless. */
    @Test
    void closingTwiceIsHarmless() {
        TcpTransportServer server = server(ECHO, TcpTransportServer.Options.defaults());
        server.close();
        server.close();
        assertThat(server.port()).isPositive(); // still reportable after the channel is gone
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
