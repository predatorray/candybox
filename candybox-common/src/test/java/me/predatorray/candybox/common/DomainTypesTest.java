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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import me.predatorray.candybox.common.exception.ValidationException;
import org.junit.jupiter.api.Test;

class DomainTypesTest {

    @Test
    void boxNameAcceptsValidNamesAndRejectsInvalid() {
        assertThat(BoxName.of("my-box-1").value()).isEqualTo("my-box-1");
        assertThatThrownBy(() -> BoxName.of("ab")).isInstanceOf(ValidationException.class);      // too short
        assertThatThrownBy(() -> BoxName.of("-box")).isInstanceOf(ValidationException.class);    // leading hyphen
        assertThatThrownBy(() -> BoxName.of("Box")).isInstanceOf(ValidationException.class);     // uppercase
        assertThatThrownBy(() -> BoxName.of("a_b_c")).isInstanceOf(ValidationException.class);   // underscore
    }

    @Test
    void candyKeyRejectsEmpty() {
        assertThatThrownBy(() -> CandyKey.of("")).isInstanceOf(ValidationException.class);
    }

    @Test
    void candyKeyOrdersByUnsignedUtf8Bytes() {
        // 'a' (0x61) < '~' (0x7e) < non-ASCII 'é' (multi-byte, first byte 0xC3) under unsigned ordering.
        List<CandyKey> sorted = new java.util.ArrayList<>(
                List.of(CandyKey.of("é"), CandyKey.of("~"), CandyKey.of("a")));
        sorted.sort(CandyKey::compareTo);
        assertThat(sorted).extracting(CandyKey::value).containsExactly("a", "~", "é");
    }

    @Test
    void candyKeyUtf8RoundTrips() {
        CandyKey original = CandyKey.of("片/路径/✓");
        CandyKey restored = CandyKey.ofUtf8(original.utf8Bytes());
        assertThat(restored).isEqualTo(original);
        assertThat(restored.value()).isEqualTo("片/路径/✓");
    }
}
