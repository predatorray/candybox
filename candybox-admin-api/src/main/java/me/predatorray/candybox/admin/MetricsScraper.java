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
package me.predatorray.candybox.admin;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import me.predatorray.candybox.common.concurrent.KeyedSlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background poller that scrapes Prometheus text endpoints (per-node {@code /metrics} or any other
 * HTTP target) on a fixed interval, parses out gauge/counter samples, and stores them in a per-
 * series rolling window. Powers {@code /api/metrics} (raw passthrough of the latest scrape) and
 * {@code /api/metrics/timeseries} (JSON of the buffered window).
 *
 * <p>Design notes:
 *
 * <ul>
 *   <li><b>In-process, no persistence.</b> Restarting the admin API clears the window — matches
 *       WEB_DASHBOARD_PLAN.md's "no time-series storage in v1" line. Operators wanting history
 *       should point Prometheus at the same endpoints.</li>
 *   <li><b>Per-target scrape isolation.</b> A flaky target logs at warn and produces no samples
 *       that tick; the others are unaffected.</li>
 *   <li><b>Bounded memory.</b> Each (name, labels) series holds at most {@code windowSamples}
 *       points (default 60 ≈ 5 minutes at 5 s polling). The total series count is bounded by what
 *       the targets emit; v1 emits a small fixed set.</li>
 * </ul>
 */
final class MetricsScraper implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsScraper.class);

    private final List<URI> targets;
    private final int windowSamples;
    private final long pollIntervalMillis;
    private final HttpClient http;
    private final ScheduledExecutorService scheduler;
    private final KeyedSlidingWindow<SeriesKey, Sample> series;
    private final String scrapeToken;
    private volatile String latestText = "";

    MetricsScraper(List<URI> targets, long pollIntervalMillis, int windowSamples) {
        this(targets, pollIntervalMillis, windowSamples, null);
    }

    /** @param scrapeToken sent as {@code Authorization: Bearer} to token-guarded /metrics targets */
    MetricsScraper(List<URI> targets, long pollIntervalMillis, int windowSamples,
                   String scrapeToken) {
        this.scrapeToken = scrapeToken;
        this.targets = List.copyOf(targets);
        this.pollIntervalMillis = pollIntervalMillis;
        this.windowSamples = windowSamples;
        this.series = new KeyedSlidingWindow<>(windowSamples);
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "admin-metrics-scraper");
            t.setDaemon(true);
            return t;
        });
    }

    void start() {
        if (targets.isEmpty()) {
            LOG.info("Metrics scraping disabled (no targets configured).");
            return;
        }
        LOG.info("Metrics scraper polling {} target(s) every {}ms", targets.size(), pollIntervalMillis);
        // Tick once on the calling thread so the first /api/metrics request after boot has data,
        // then schedule the periodic refresh.
        scrapeOnce();
        scheduler.scheduleAtFixedRate(this::scrapeOnce, pollIntervalMillis, pollIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    /** Returns the most recent raw exposition text, concatenated across targets. */
    String latestText() {
        return latestText;
    }

    /**
     * Returns rolling-window samples for the given metric names. A name that has no samples yet
     * (or doesn't appear in scrape output) yields an empty series — the caller can render an
     * "awaiting data" placeholder without special-casing.
     */
    Map<String, List<Series>> seriesFor(Collection<String> names) {
        Map<String, List<Series>> out = new LinkedHashMap<>();
        Map<SeriesKey, List<Sample>> snapshot = series.snapshot();
        for (String name : names) {
            List<Series> matches = new ArrayList<>();
            for (Map.Entry<SeriesKey, List<Sample>> e : snapshot.entrySet()) {
                if (!e.getKey().name.equals(name)) {
                    continue;
                }
                matches.add(new Series(e.getKey().name, e.getKey().labels, e.getValue()));
            }
            out.put(name, matches);
        }
        return out;
    }

    int windowSeconds() {
        return (int) ((windowSamples * pollIntervalMillis) / 1000L);
    }

    // ---- scraping ------------------------------------------------------------------------------

    private void scrapeOnce() {
        StringBuilder concat = new StringBuilder();
        long ingestMillis = System.currentTimeMillis();
        for (URI target : targets) {
            try {
                HttpRequest.Builder rb = HttpRequest.newBuilder(target)
                        .timeout(Duration.ofSeconds(3)).header("Accept", "text/plain").GET();
                if (scrapeToken != null) {
                    rb.header("Authorization", "Bearer " + scrapeToken);
                }
                HttpResponse<String> resp = http.send(rb.build(),
                        HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() != 200) {
                    LOG.debug("metrics target {} returned {}", target, resp.statusCode());
                    continue;
                }
                concat.append("# target=").append(target).append('\n');
                concat.append(resp.body());
                if (!resp.body().endsWith("\n")) {
                    concat.append('\n');
                }
                ingest(resp.body(), ingestMillis);
            } catch (Exception e) {
                // Loud-but-not-fatal: log once per failure but keep ticking. A flapping target
                // shouldn't drown the log either, so debug-log the trace and warn just the message.
                LOG.warn("Failed to scrape {}: {}", target, e.toString());
                LOG.debug("scrape failure detail", e);
            }
        }
        latestText = concat.toString();
    }

    private void ingest(String text, long ingestMillis) {
        for (PrometheusText.Sample s : PrometheusText.parse(text)) {
            series.append(new SeriesKey(s.name(), s.labels()), new Sample(ingestMillis, s.value()));
        }
    }

    // ---- shapes --------------------------------------------------------------------------------

    /** One stored sample: ingest timestamp + value. */
    record Sample(long t, double v) {
    }

    /** One series view returned to the API layer. */
    record Series(String name, Map<String, String> labels, List<Sample> samples) {
    }

    /** Map key for the rolling-window store — name + label fingerprint. */
    private static final class SeriesKey {
        final String name;
        final Map<String, String> labels;

        SeriesKey(String name, Map<String, String> labels) {
            this.name = name;
            this.labels = labels;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SeriesKey k)) {
                return false;
            }
            return name.equals(k.name) && labels.equals(k.labels);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, labels);
        }
    }
}
