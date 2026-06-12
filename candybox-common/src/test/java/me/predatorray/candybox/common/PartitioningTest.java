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

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PartitioningTest {

    @Test
    void deterministicAndInRange() {
        for (int n : new int[] {1, 2, 8, 17}) {
            for (int i = 0; i < 200; i++) {
                String key = "key/" + i;
                int p = Partitioning.partitionOf(key, n);
                assertThat(p).isBetween(0, n - 1);
                // Stable across repeated calls and across the String/byte[] overloads (clients and
                // servers must always agree).
                assertThat(Partitioning.partitionOf(key, n)).isEqualTo(p);
                assertThat(Partitioning.partitionOf(key.getBytes(StandardCharsets.UTF_8), n))
                        .isEqualTo(p);
            }
        }
    }

    @Test
    void spreadsKeysAcrossPartitions() {
        Set<Integer> used = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            used.add(Partitioning.partitionOf("key/" + i, 8));
        }
        assertThat(used).hasSize(8); // 200 keys over 8 buckets must touch every bucket
    }

    @Test
    void singlePartitionMapsEverythingToZero() {
        assertThat(Partitioning.partitionOf("anything", 1)).isZero();
    }

    @Test
    void rejectsNonPositivePartitionCount() {
        assertThatThrownBy(() -> Partitioning.partitionOf("k", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Partitioning.partitionOf("k", -3))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
