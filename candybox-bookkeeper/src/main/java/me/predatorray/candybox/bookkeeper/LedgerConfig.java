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
package me.predatorray.candybox.bookkeeper;

import java.util.Map;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.config.QuorumConfig;

/**
 * Parameters for creating a ledger: its E/Qw/Qa quorum and any custom metadata stamped on the ledger
 * (e.g. its Candybox role, owning Box, fencing epoch).
 *
 * @param quorum         ensemble / write-quorum / ack-quorum
 * @param customMetadata immutable custom metadata to attach at creation
 */
public record LedgerConfig(QuorumConfig quorum, Map<String, byte[]> customMetadata) {

    public LedgerConfig {
        if (quorum == null) {
            throw new IllegalArgumentException("quorum is required");
        }
        customMetadata = customMetadata == null ? Map.of() : Map.copyOf(customMetadata);
    }

    public LedgerConfig(QuorumConfig quorum) {
        this(quorum, Map.of());
    }

    /** Convenience: default quorum for a role with no custom metadata. */
    public static LedgerConfig forRole(LedgerRole role) {
        return new LedgerConfig(QuorumConfig.defaultFor(role));
    }
}
