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
package me.predatorray.candybox.s3;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EtagTest {

    @Test
    void formatsAsQuoted32HexFromCrc32c() {
        // Left-padded to 32 hex chars so it has the visual shape of an MD5 ETag.
        assertThat(Etag.of(0x8f3a2b1c)).isEqualTo("\"0000000000000000000000008f3a2b1c\"");
        assertThat(Etag.unquoted(0x8f3a2b1c)).isEqualTo("0000000000000000000000008f3a2b1c");
        assertThat(Etag.unquoted(0x8f3a2b1c)).hasSize(32);
    }

    @Test
    void treatsCrcAsUnsigned32Bit() {
        // 0xffffffff must not become a negative/sign-extended value.
        assertThat(Etag.unquoted(0xffffffff)).isEqualTo("000000000000000000000000ffffffff");
        assertThat(Etag.unquoted(0)).isEqualTo("00000000000000000000000000000000");
    }
}
