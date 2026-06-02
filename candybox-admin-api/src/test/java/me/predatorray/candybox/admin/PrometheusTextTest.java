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

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PrometheusTextTest {

    @Test
    void parsesBareSampleAndLabels() {
        String text =
                "# HELP candybox_owned_boxes Owned box count.\n"
                + "# TYPE candybox_owned_boxes gauge\n"
                + "candybox_owned_boxes 3\n"
                + "candybox_request_count{op=\"get\"} 12345\n"
                + "candybox_request_count{op=\"put\",code=\"200\"} 678\n";
        List<PrometheusText.Sample> samples = PrometheusText.parse(text);
        assertThat(samples).hasSize(3);
        assertThat(samples.get(0).name()).isEqualTo("candybox_owned_boxes");
        assertThat(samples.get(0).labels()).isEmpty();
        assertThat(samples.get(0).value()).isEqualTo(3.0);
        assertThat(samples.get(1).labels()).isEqualTo(Map.of("op", "get"));
        assertThat(samples.get(2).labels()).isEqualTo(Map.of("op", "put", "code", "200"));
    }

    @Test
    void ignoresCommentsBlankLinesAndUnparseable() {
        String text = "\n#\n# HELP …\ngarbage line without number\n";
        assertThat(PrometheusText.parse(text)).isEmpty();
    }

    @Test
    void rejectsNonFiniteValues() {
        String text =
                "good 1.5\n"
                + "naan NaN\n"
                + "infty +Inf\n"
                + "minus -Inf\n";
        List<PrometheusText.Sample> samples = PrometheusText.parse(text);
        assertThat(samples).extracting(PrometheusText.Sample::name).containsExactly("good");
    }

    @Test
    void ignoresTrailingTimestamp() {
        // Prometheus expositions may carry a trailing millisecond timestamp; we drop it because
        // the scraper assigns its own clock for cross-target consistency.
        List<PrometheusText.Sample> samples = PrometheusText.parse("a_metric 7 1700000000000\n");
        assertThat(samples).hasSize(1);
        assertThat(samples.get(0).value()).isEqualTo(7.0);
    }

    @Test
    void handlesEscapedLabelValues() {
        List<PrometheusText.Sample> samples = PrometheusText.parse(
                "m{path=\"/api/\\\"q\\\"\"} 1\n");
        assertThat(samples).hasSize(1);
        assertThat(samples.get(0).labels().get("path")).isEqualTo("/api/\"q\"");
    }
}
