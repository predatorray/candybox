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
package me.predatorray.candybox.common.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BytesTest {

    private static byte[] bytes(int... unsigned) {
        byte[] out = new byte[unsigned.length];
        for (int i = 0; i < unsigned.length; i++) {
            out[i] = (byte) unsigned[i];
        }
        return out;
    }

    @Test
    void compareIsUnsignedSoHighBytesSortAfterLowBytes() {
        // 0x80 (128 unsigned) must sort after 0x7F (127), not before (as it would signed).
        assertThat(Bytes.compare(bytes(0x80), bytes(0x7F))).isPositive();
        assertThat(Bytes.compare(bytes(0xFF), bytes(0x00))).isPositive();
    }

    @Test
    void compareEqualArraysReturnsZero() {
        assertThat(Bytes.compare(bytes(1, 2, 3), bytes(1, 2, 3))).isZero();
    }

    @Test
    void compareUsesLengthAsTieBreakerWhenOneIsAPrefixOfTheOther() {
        assertThat(Bytes.compare(bytes(1, 2), bytes(1, 2, 3))).isNegative();
        assertThat(Bytes.compare(bytes(1, 2, 3), bytes(1, 2))).isPositive();
    }

    @Test
    void emptyArraySortsBeforeNonEmpty() {
        assertThat(Bytes.compare(bytes(), bytes(0x00))).isNegative();
        assertThat(Bytes.compare(bytes(), bytes())).isZero();
    }

    @Test
    void lexicographicComparatorDelegatesToCompare() {
        assertThat(Bytes.LEXICOGRAPHIC.compare(bytes(0x80), bytes(0x7F))).isPositive();
        assertThat(Bytes.LEXICOGRAPHIC.compare(bytes(1), bytes(1))).isZero();
    }

    @Test
    void prefixSuccessorIncrementsLastByte() {
        assertThat(Bytes.prefixSuccessor(bytes('a', 'b'))).containsExactly('a', 'c');
    }

    @Test
    void prefixSuccessorDropsTrailingFfRunAndIncrementsTheByteBelowIt() {
        // {0x01, 0xFF, 0xFF} -> {0x02}: the trailing 0xFF run is dropped, the byte below it bumped.
        assertThat(Bytes.prefixSuccessor(bytes(0x01, 0xFF, 0xFF))).containsExactly(0x02);
    }

    @Test
    void prefixSuccessorOfEmptyPrefixIsUnbounded() {
        assertThat(Bytes.prefixSuccessor(bytes())).isNull();
    }

    @Test
    void prefixSuccessorOfAllFfIsUnbounded() {
        assertThat(Bytes.prefixSuccessor(bytes(0xFF, 0xFF))).isNull();
    }

    @Test
    void prefixSuccessorIsStrictUpperBoundOfTheRange() {
        byte[] prefix = bytes('k', 'e', 'y');
        byte[] successor = Bytes.prefixSuccessor(prefix);
        assertThat(successor).isNotNull();
        // Every array starting with the prefix sorts strictly below the successor.
        assertThat(Bytes.compare(prefix, successor)).isNegative();
        assertThat(Bytes.compare(bytes('k', 'e', 'y', 0x00), successor)).isNegative();
        assertThat(Bytes.compare(bytes('k', 'e', 'y', (byte) 0xFF), successor)).isNegative();
    }
}
