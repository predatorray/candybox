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
