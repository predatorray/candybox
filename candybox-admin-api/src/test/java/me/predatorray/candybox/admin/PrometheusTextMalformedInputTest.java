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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PrometheusTextMalformedInputTest {
    @ParameterizedTest
    @ValueSource(strings = {"{a=\"b\"} 1", "m{a=\"b\" 1", "m{a=\"b\"}", "metricOnly", "m nope",
            "m NaN", "m Infinity", "m -Infinity"})
    void badLineDoesNotDropNeighboringSamples(String bad) {
        assertThat(PrometheusText.parse("before 1\n" + bad + "\nafter 2\n"))
                .containsExactly(new PrometheusText.Sample("before", Map.of(), 1),
                        new PrometheusText.Sample("after", Map.of(), 2));
    }

    @Test
    void unescapesPrometheusLabelsAndKeepsUnknownEscapesLenient() {
        assertThat(PrometheusText.parse("m{a=\"x\\\"y\",b=\"p\\\\q\",c=\"l1\\nl2\",d=\"\\z\"} 3"))
                .containsExactly(new PrometheusText.Sample("m",
                        Map.of("a", "x\"y", "b", "p\\q", "c", "l1\nl2", "d", "z"), 3));
    }

    @ParameterizedTest
    @ValueSource(strings = {"m{a=\"1\",} 2", "m{a=\"1\",  } 2", "m{ a=\"1\",b=2} 2",
            "m{a=\"1\",bad} 2", "m{a=\"1\",b=\"unfinished} 2", "m{a=\"1\",b=\"slash\\} 2"})
    void malformedLabelTailPreservesEarlierLabelsAndNeighboringSamples(String line) {
        assertThat(PrometheusText.parse("before 1\n" + line + "\nafter 2"))
                .containsExactly(new PrometheusText.Sample("before", Map.of(), 1),
                        new PrometheusText.Sample("m", Map.of("a", "1"), 2),
                        new PrometheusText.Sample("after", Map.of(), 2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"m{a=1} 2", "m{=} 2", "m{} 2", "m{bare} 2", "m{a=\"unfinished} 2"})
    void malformedLabelsStillAllowSampleWithEmptyLabels(String line) {
        assertThat(PrometheusText.parse("before 1\n" + line + "\nafter 2"))
                .containsExactly(new PrometheusText.Sample("before", Map.of(), 1),
                        new PrometheusText.Sample("m", Map.of(), 2),
                        new PrometheusText.Sample("after", Map.of(), 2));
    }

    @Test
    void acceptsWhitespaceEmptyValuesAndOptionalTimestamp() {
        assertThat(PrometheusText.parse("\n  # comment\n \tm\t2\t123\n n{a=\"\"} 3 \n"))
                .containsExactly(new PrometheusText.Sample("m", Map.of(), 2),
                        new PrometheusText.Sample("n", Map.of("a", ""), 3));
        assertThat(PrometheusText.parse("m{a=\"1\"}   \n")).isEmpty();
    }
}
