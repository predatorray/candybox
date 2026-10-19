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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.auth.Grant;
import me.predatorray.candybox.common.auth.ObjectAcl;
import me.predatorray.candybox.common.exception.SerializationException;
import org.junit.jupiter.api.Test;

class LocatorRelayTest {

    private static final CandyLocator LOCATOR = new CandyLocator(new Hlc(1000, 2, 3),
            LocatorType.PUT, "text/plain", Map.of("m", "v"), 1234L,
            List.of(new Part(7, 1 << 20, 0x1234, List.of(new SegmentRef(42, 0, 3)))),
            new ObjectAcl("User:alice", List.of(Grant.parse("AllUsers:READ"))));

    @Test
    void renameRelayRoundTrips() {
        LocatorRelay relay = new LocatorRelay("User:alice", "src", "tok", "dst", 99L, LOCATOR);
        assertThat(LocatorRelay.deserialize(relay.serialize())).isEqualTo(relay);
    }

    @Test
    void copyRelayRoundTrips() {
        LocatorRelay relay = new LocatorRelay("User:ANONYMOUS", "src", null, null, 99L, LOCATOR);
        LocatorRelay decoded = LocatorRelay.deserialize(relay.serialize());
        assertThat(decoded).isEqualTo(relay);
        assertThat(decoded.isRename()).isFalse();
    }

    @Test
    void unknownVersionAndTruncatedBytesAreRejected() {
        byte[] bytes = new LocatorRelay("User:alice", "src", null, null, 99L, LOCATOR).serialize();
        byte[] future = bytes.clone();
        future[0] = 2;
        assertThatThrownBy(() -> LocatorRelay.deserialize(future))
                .isInstanceOf(SerializationException.class);
        assertThatThrownBy(() -> LocatorRelay.deserialize(Arrays.copyOf(bytes, bytes.length / 2)))
                .isInstanceOf(SerializationException.class);
    }
}
