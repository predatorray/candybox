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
package me.predatorray.candybox.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.LimitExceededException;
import org.junit.jupiter.api.Test;

class ValidationTest {

    /** A SizeLimits with tight bounds so the limits are easy to cross deterministically. */
    private static SizeLimits tight(int maxKeyBytes, int maxMetadataBytes, long maxCandyBytes) {
        return new SizeLimits(1 << 20, maxKeyBytes, maxMetadataBytes, 64 << 10, maxCandyBytes);
    }

    @Test
    void candyKeyWithinLimitPasses() {
        SizeLimits limits = tight(8, 8 << 10, 0);
        assertThatCode(() -> Validation.checkCandyKey(CandyKey.of("12345678"), limits))
                .doesNotThrowAnyException();
    }

    @Test
    void candyKeyOverLimitIsRejected() {
        SizeLimits limits = tight(4, 8 << 10, 0);
        assertThatThrownBy(() -> Validation.checkCandyKey(CandyKey.of("12345"), limits))
                .isInstanceOf(LimitExceededException.class)
                .hasMessageContaining("CandyKey");
    }

    @Test
    void candyKeyByteLengthCountsUtf8NotChars() {
        // Each "é" is two UTF-8 bytes: two of them are 4 bytes, which exceeds a 3-byte limit.
        SizeLimits limits = tight(3, 8 << 10, 0);
        assertThatThrownBy(() -> Validation.checkCandyKey(CandyKey.of("éé"), limits))
                .isInstanceOf(LimitExceededException.class);
    }

    @Test
    void nullMetadataIsTreatedAsEmpty() {
        SizeLimits limits = tight(1 << 10, 0, 0);
        assertThatCode(() -> Validation.checkUserMetadata(null, limits))
                .doesNotThrowAnyException();
    }

    @Test
    void metadataSizeSumsKeyAndValueBytesAcrossEntries() {
        // "aa"+"bb" + "cc"+"dd" = 8 bytes total.
        Map<String, String> metadata = Map.of("aa", "bb", "cc", "dd");
        assertThatCode(() -> Validation.checkUserMetadata(metadata, tight(1 << 10, 8, 0)))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> Validation.checkUserMetadata(metadata, tight(1 << 10, 7, 0)))
                .isInstanceOf(LimitExceededException.class)
                .hasMessageContaining("metadata");
    }

    @Test
    void candySizeWithinBoundedLimitPasses() {
        assertThatCode(() -> Validation.checkCandySize(100, tight(1 << 10, 8 << 10, 100)))
                .doesNotThrowAnyException();
    }

    @Test
    void candySizeOverBoundedLimitIsRejected() {
        assertThatThrownBy(() -> Validation.checkCandySize(101, tight(1 << 10, 8 << 10, 100)))
                .isInstanceOf(LimitExceededException.class)
                .hasMessageContaining("Candy size");
    }

    @Test
    void candySizeIsUnboundedWhenLimitIsZero() {
        assertThatCode(() -> Validation.checkCandySize(Long.MAX_VALUE, tight(1 << 10, 8 << 10, 0)))
                .doesNotThrowAnyException();
    }

    @Test
    void emptyMetadataMapPasses() {
        assertThat(Map.<String, String>of()).isEmpty();
        assertThatCode(() -> Validation.checkUserMetadata(Map.of(), tight(1 << 10, 0, 0)))
                .doesNotThrowAnyException();
    }
}
