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

import java.time.Duration;
import me.predatorray.candybox.protocol.transport.TcpTransportServer.Options;
import org.junit.jupiter.api.Test;

/**
 * The listener's sizing knobs. These come from operator-supplied configuration, so a value that
 * would silently cripple the node — no handler threads, no pipelining allowance — is rejected at
 * construction rather than surfacing later as a node that accepts connections and answers nothing.
 */
class TransportServerOptionsTest {

    @Test
    void defaultsAreUsable() {
        Options defaults = Options.defaults();

        assertThat(defaults.ioThreads()).isZero(); // 0 ⇒ Netty's own default
        assertThat(defaults.handlerThreads()).isGreaterThanOrEqualTo(32);
        assertThat(defaults.maxInFlightPerConnection()).isPositive();
        assertThat(defaults.drainTimeout()).isPositive();
    }

    @Test
    void rejectsNegativeIoThreads() {
        assertThatThrownBy(() -> new Options(-1, 4, 8, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ioThreads must be non-negative");
    }

    @Test
    void rejectsAHandlerPoolThatCannotRunAnything() {
        assertThatThrownBy(() -> new Options(1, 0, 8, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("handlerThreads must be positive");
    }

    /** A limit of zero would gate every connection shut on its first request. */
    @Test
    void rejectsAnInFlightLimitOfZero() {
        assertThatThrownBy(() -> new Options(1, 4, 0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightPerConnection must be positive");
    }

    @Test
    void rejectsAnAbsentOrNegativeDrainTimeout() {
        assertThatThrownBy(() -> new Options(1, 4, 8, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("drainTimeout must be non-negative");
        assertThatThrownBy(() -> new Options(1, 4, 8, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("drainTimeout must be non-negative");
    }

    /** Zero is allowed and means "do not wait": a shutdown that wants to cut its losses. */
    @Test
    void allowsAZeroDrainTimeout() {
        assertThat(new Options(1, 4, 8, Duration.ZERO).drainTimeout()).isZero();
    }
}
