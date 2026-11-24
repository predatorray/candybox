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
package me.predatorray.candybox.coordination.zk;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LocalLeaseValidityTest {
    @Test
    void latencyConsumesBudgetAndBackwardWallClockCannotExtendIt() {
        LocalLeaseValidity validity = new LocalLeaseValidity(0, 1_000, 2_000);
        assertThat(validity.isValid(ms(949), 1_500)).isTrue();
        assertThat(validity.isValid(ms(950), 500)).isFalse();
        assertThat(validity.isValid(ms(100), 2_000)).isFalse();
    }

    @Test
    void nanoTimeWraparoundAndTinyTtlFailConservatively() {
        long start = Long.MAX_VALUE - ms(100);
        LocalLeaseValidity validity = new LocalLeaseValidity(start, 1_000, 2_000);
        assertThat(validity.isValid(start + ms(949), 1_000)).isTrue();
        assertThat(validity.isValid(start + ms(950), 1_000)).isFalse();
        assertThat(new LocalLeaseValidity(0, 1, 2_000).isValid(0, 1_000)).isFalse();
    }

    private static long ms(long value) { return TimeUnit.MILLISECONDS.toNanos(value); }
}
