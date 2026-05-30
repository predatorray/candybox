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
package me.predatorray.candybox.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Covers the argument-handling paths of {@link CandyboxCli} that resolve before any network call —
 * usage/help, missing arguments and a malformed {@code --server}. Command execution against a live
 * node is exercised end-to-end in the integration tests.
 */
class CandyboxCliTest {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private int run(String... args) {
        return CandyboxCli.run(args, new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    private String stdout() {
        return out.toString(StandardCharsets.UTF_8);
    }

    private String stderr() {
        return err.toString(StandardCharsets.UTF_8);
    }

    @Test
    void noArgumentsPrintsUsageAndFails() {
        assertThat(run()).isEqualTo(2);
        assertThat(stdout()).contains("Usage: candybox");
    }

    @Test
    void helpPrintsUsageAndSucceeds() {
        assertThat(run("help")).isEqualTo(0);
        assertThat(stdout()).contains("create-box").contains("list-boxes");
    }

    @Test
    void missingServerValueIsRejected() {
        assertThat(run("-s")).isEqualTo(2);
        assertThat(stderr()).contains("Missing value for -s");
    }

    @Test
    void malformedServerIsRejected() {
        assertThat(run("-s", "no-port", "list-boxes")).isEqualTo(2);
        assertThat(stderr()).contains("Invalid --server");
    }
}
