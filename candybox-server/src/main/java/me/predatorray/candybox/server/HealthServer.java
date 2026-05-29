package me.predatorray.candybox.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.BooleanSupplier;
import me.predatorray.candybox.lsm.engine.BoxEngineStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tiny HTTP endpoint for orchestration, built on the JDK's {@link HttpServer} so it adds no
 * dependency. Serves:
 *
 * <ul>
 *   <li>{@code GET /healthz} — process liveness; always {@code 200} while the JVM is up.</li>
 *   <li>{@code GET /readyz} — readiness; {@code 200} when the supplied predicate is true (TCP bound
 *       and membership registered), else {@code 503}. Wire this to a Kubernetes readiness probe.</li>
 *   <li>{@code GET /metrics} — Prometheus text exposition of {@link BoxEngineStats} counters, one
 *       series per owned Box (a {@code box} label) plus a {@code node} label.</li>
 * </ul>
 */
public final class HealthServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HealthServer.class);

    private final HttpServer http;

    /**
     * @param port        the HTTP port to bind
     * @param nodeId      this node's id, emitted as a {@code node} metric label
     * @param ready       readiness predicate
     * @param statsSource supplies a per-Box stats snapshot at scrape time
     */
    public HealthServer(int port, int nodeId, BooleanSupplier ready,
                        java.util.function.Supplier<Map<String, BoxEngineStats>> statsSource) {
        try {
            this.http = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bind health server on port " + port, e);
        }
        http.createContext("/healthz", exchange -> respond(exchange, 200, "ok\n"));
        http.createContext("/readyz", exchange -> {
            boolean ok = ready.getAsBoolean();
            respond(exchange, ok ? 200 : 503, ok ? "ready\n" : "not ready\n");
        });
        http.createContext("/metrics", exchange ->
                respond(exchange, 200, renderMetrics(nodeId, statsSource.get())));
        http.setExecutor(null); // default executor (a small internal pool)
    }

    public void start() {
        http.start();
        LOG.info("Health/metrics endpoint listening on port {}", http.getAddress().getPort());
    }

    public int port() {
        return http.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** Renders the per-Box counters in Prometheus text exposition format. */
    static String renderMetrics(int nodeId, Map<String, BoxEngineStats> byBox) {
        StringBuilder sb = new StringBuilder();
        metric(sb, "candybox_puts_total", "Total Candy puts.", nodeId, byBox, BoxEngineStats::puts);
        metric(sb, "candybox_deletes_total", "Total Candy deletes.", nodeId, byBox, BoxEngineStats::deletes);
        metric(sb, "candybox_gets_total", "Total Candy gets.", nodeId, byBox, BoxEngineStats::gets);
        metric(sb, "candybox_heads_total", "Total Candy heads.", nodeId, byBox, BoxEngineStats::heads);
        metric(sb, "candybox_lists_total", "Total Candy lists.", nodeId, byBox, BoxEngineStats::lists);
        metric(sb, "candybox_flushes_total", "Total memtable flushes.", nodeId, byBox, BoxEngineStats::flushes);
        metric(sb, "candybox_compactions_total", "Total compactions.", nodeId, byBox, BoxEngineStats::compactions);
        metric(sb, "candybox_stall_rejections_total", "Total writes rejected for L0 stall.", nodeId,
                byBox, BoxEngineStats::stallRejections);
        sb.append("# HELP candybox_owned_boxes Number of Boxes this node currently owns.\n");
        sb.append("# TYPE candybox_owned_boxes gauge\n");
        sb.append("candybox_owned_boxes{node=\"").append(nodeId).append("\"} ")
                .append(byBox.size()).append('\n');
        return sb.toString();
    }

    private static void metric(StringBuilder sb, String name, String help, int nodeId,
                               Map<String, BoxEngineStats> byBox,
                               java.util.function.ToLongFunction<BoxEngineStats> field) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" counter\n");
        for (Map.Entry<String, BoxEngineStats> e : byBox.entrySet()) {
            sb.append(name).append("{node=\"").append(nodeId).append("\",box=\"")
                    .append(escape(e.getKey())).append("\"} ")
                    .append(field.applyAsLong(e.getValue())).append('\n');
        }
    }

    private static String escape(String label) {
        return label.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    @Override
    public void close() {
        http.stop(0);
    }
}
