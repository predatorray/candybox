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
package me.predatorray.candybox.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import me.predatorray.candybox.common.Partitioning;
import org.junit.jupiter.api.Test;

class BoxDescriptorTest {

    @Test
    void encodeDecodeRoundTrips() {
        for (int count : new int[] {1, 8, 1024}) {
            BoxDescriptor descriptor = new BoxDescriptor(count);
            assertThat(BoxDescriptor.decode(descriptor.encode())).isEqualTo(descriptor);
        }
    }

    @Test
    void partitionOfMatchesTheSharedHashFunction() {
        BoxDescriptor descriptor = new BoxDescriptor(8);
        assertThat(descriptor.partitionOf("some/key"))
                .isEqualTo(Partitioning.partitionOf("some/key", 8));
    }

    @Test
    void rejectsNonPositivePartitionCount() {
        assertThatThrownBy(() -> new BoxDescriptor(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUnknownFormatVersion() {
        byte[] encoded = new BoxDescriptor(4).encode();
        encoded[0] = 99;
        assertThatThrownBy(() -> BoxDescriptor.decode(encoded))
                .isInstanceOf(CoordinationException.class);
    }
}
