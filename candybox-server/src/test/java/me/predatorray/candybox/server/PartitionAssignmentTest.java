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
package me.predatorray.candybox.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import me.predatorray.candybox.server.PartitionAssignment.BoxPartition;
import org.junit.jupiter.api.Test;

class PartitionAssignmentTest {

    @Test
    void encodeDecodeRoundTripsTheTargets() {
        PartitionAssignment assignment = new PartitionAssignment(Map.of(
                new BoxPartition("alpha", 0), 1,
                new BoxPartition("alpha", 1), 2,
                new BoxPartition("beta", 0), 1));
        PartitionAssignment decoded = PartitionAssignment.decode(assignment.encode());
        assertThat(decoded.targets()).isEqualTo(assignment.targets());

        assertThat(PartitionAssignment.decode(PartitionAssignment.empty().encode()).targets())
                .isEmpty();
    }

    @Test
    void rejectsUnknownFormatVersion() {
        byte[] encoded = PartitionAssignment.empty().encode();
        encoded[0] = 9;
        assertThatThrownBy(() -> PartitionAssignment.decode(encoded))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void boxPartitionsOrderByBoxThenPartition() {
        assertThat(new BoxPartition("a", 1)).isLessThan(new BoxPartition("b", 0));
        assertThat(new BoxPartition("a", 1)).isGreaterThan(new BoxPartition("a", 0));
        assertThat(new BoxPartition("a", 1)).isEqualByComparingTo(new BoxPartition("a", 1));
    }
}
