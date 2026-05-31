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
package me.predatorray.candybox.common.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SizeLimitsTest {

    @Test
    void defaultsMatchDocumentedConstants() {
        SizeLimits limits = SizeLimits.defaults();
        assertThat(limits.chunkSizeBytes()).isEqualTo(SizeLimits.DEFAULT_CHUNK_SIZE);
        assertThat(limits.maxCandyKeyBytes()).isEqualTo(SizeLimits.DEFAULT_MAX_KEY_BYTES);
        assertThat(limits.maxUserMetadataBytes()).isEqualTo(SizeLimits.DEFAULT_MAX_METADATA_BYTES);
        assertThat(limits.maxLocatorBytes()).isEqualTo(SizeLimits.DEFAULT_MAX_LOCATOR_BYTES);
        assertThat(limits.maxCandySizeBytes()).isEqualTo(SizeLimits.DEFAULT_MAX_CANDY_SIZE);
    }

    @Test
    void unboundedCandySizeAllowsAnyLength() {
        SizeLimits limits = SizeLimits.defaults(); // maxCandySizeBytes == 0 == unbounded
        assertThat(limits.isCandySizeAllowed(0)).isTrue();
        assertThat(limits.isCandySizeAllowed(Long.MAX_VALUE)).isTrue();
    }

    @Test
    void boundedCandySizeEnforcesInclusiveMaximum() {
        SizeLimits limits = new SizeLimits(1 << 20, 1 << 10, 8 << 10, 64 << 10, 100);
        assertThat(limits.isCandySizeAllowed(99)).isTrue();
        assertThat(limits.isCandySizeAllowed(100)).isTrue();   // inclusive
        assertThat(limits.isCandySizeAllowed(101)).isFalse();
    }

    @Test
    void rejectsNonPositiveChunkSize() {
        assertThatThrownBy(() -> new SizeLimits(0, 1, 1, 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chunkSizeBytes");
    }

    @Test
    void rejectsNonPositiveKeyLimit() {
        assertThatThrownBy(() -> new SizeLimits(1, 0, 1, 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxCandyKeyBytes");
    }

    @Test
    void rejectsNegativeMetadataLimitButAllowsZero() {
        assertThatThrownBy(() -> new SizeLimits(1, 1, -1, 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metadata/locator");
        assertThat(new SizeLimits(1, 1, 0, 1, 0).maxUserMetadataBytes()).isZero();
    }

    @Test
    void rejectsNonPositiveLocatorLimit() {
        assertThatThrownBy(() -> new SizeLimits(1, 1, 1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metadata/locator");
    }

    @Test
    void rejectsNegativeMaxCandySize() {
        assertThatThrownBy(() -> new SizeLimits(1, 1, 1, 1, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxCandySizeBytes");
    }
}
