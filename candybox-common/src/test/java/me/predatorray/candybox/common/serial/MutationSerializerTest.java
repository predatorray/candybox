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
package me.predatorray.candybox.common.serial;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.SegmentRef;
import org.junit.jupiter.api.Test;

class MutationSerializerTest {

    @Test
    void roundTripsPutAndTombstoneMutations() {
        Mutation put = new Mutation(CandyKey.of("path/to/candy"),
                new CandyLocator(new Hlc(10, 1, 2), LocatorType.PUT, 42, 1024, "text/plain",
                        Map.of("k", "v"), 7, 5, List.of(new SegmentRef(3, 0, 0))));
        Mutation delete = new Mutation(CandyKey.of("path/to/candy"),
                CandyLocator.tombstone(new Hlc(20, 0, 2), 6));

        assertThat(MutationSerializer.deserialize(MutationSerializer.serialize(put))).isEqualTo(put);
        assertThat(MutationSerializer.deserialize(MutationSerializer.serialize(delete))).isEqualTo(delete);
    }
}
