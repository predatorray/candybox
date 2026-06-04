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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class AdminApiMainTest {

    @Test
    void defaultsWhenEnvIsEmpty() {
        AdminApiConfig c = AdminApiMain.fromEnv(Map.of());
        assertThat(c.port()).isEqualTo(AdminApiConfig.DEFAULT_PORT);
        assertThat(c.bindAddress()).isEqualTo("0.0.0.0");
        assertThat(c.corsAllowOrigin()).isEqualTo("*");
        assertThat(c.uiEnabled()).isTrue();
    }

    @Test
    void honoursEnvOverrides() {
        AdminApiConfig c = AdminApiMain.fromEnv(Map.of(
                "CANDYBOX_ADMIN_PORT", "19712",
                "CANDYBOX_ADMIN_BIND", "127.0.0.1",
                "CANDYBOX_ADMIN_CORS", "https://example.com",
                "CANDYBOX_ADMIN_UI", "false"));
        assertThat(c.port()).isEqualTo(19712);
        assertThat(c.bindAddress()).isEqualTo("127.0.0.1");
        assertThat(c.corsAllowOrigin()).isEqualTo("https://example.com");
        assertThat(c.uiEnabled()).isFalse();
    }

    @Test
    void ignoresNonNumericPort() {
        AdminApiConfig c = AdminApiMain.fromEnv(Map.of("CANDYBOX_ADMIN_PORT", "not-a-port"));
        assertThat(c.port()).isEqualTo(AdminApiConfig.DEFAULT_PORT);
    }

    @Test
    void blankEnvOverridesFallBackToDefaults() {
        // Setting an env var to the empty string is a common mistake — should be treated as
        // "not set" rather than overriding the default bind to "".
        AdminApiConfig c = AdminApiMain.fromEnv(Map.of(
                "CANDYBOX_ADMIN_PORT", "  ",
                "CANDYBOX_ADMIN_BIND", "",
                "CANDYBOX_ADMIN_CORS", ""));
        assertThat(c.port()).isEqualTo(AdminApiConfig.DEFAULT_PORT);
        assertThat(c.bindAddress()).isEqualTo("0.0.0.0");
        assertThat(c.corsAllowOrigin()).isEqualTo("*");
    }

    @Test
    void scraperIsNullWhenNoTargets() {
        // No env, empty env, only-whitespace env — all three must yield null so AdminApiMain skips
        // wiring the scraper into the server.
        assertThat(AdminApiMain.buildScraper(Map.of())).isNull();
        assertThat(AdminApiMain.buildScraper(Map.of("CANDYBOX_ADMIN_SCRAPE_TARGETS", ""))).isNull();
        assertThat(AdminApiMain.buildScraper(Map.of("CANDYBOX_ADMIN_SCRAPE_TARGETS", " , , "))).isNull();
    }

    @Test
    void scraperParsesCommaSeparatedTargetsAndIntervalWindow() {
        MetricsScraper scraper = AdminApiMain.buildScraper(Map.of(
                "CANDYBOX_ADMIN_SCRAPE_TARGETS",
                "http://a:9710/metrics, http://b:9710/metrics",
                "CANDYBOX_ADMIN_SCRAPE_INTERVAL_MS", "2000",
                "CANDYBOX_ADMIN_SCRAPE_WINDOW", "30"));
        assertThat(scraper).isNotNull();
        // 30 samples * 2000ms / 1000 = 60s rolling window.
        assertThat(scraper.windowSeconds()).isEqualTo(60);
        // Don't start() it — no real targets to scrape against.
        scraper.close();
    }

    @Test
    void scraperSkipsMalformedTargetsButKeepsValidOnes() {
        // ":://bad uri" is unparseable; "http://ok:9710/metrics" is fine — the malformed one
        // should be logged and dropped, not crash the whole build.
        MetricsScraper scraper = AdminApiMain.buildScraper(Map.of(
                "CANDYBOX_ADMIN_SCRAPE_TARGETS",
                ":://bad uri, http://ok:9710/metrics"));
        assertThat(scraper).isNotNull();
        scraper.close();
    }
}
