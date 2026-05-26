package me.predatorray.candybox.common.config;

import java.util.Map;

/**
 * BookKeeper ensemble / write-quorum / ack-quorum (E/Qw/Qa) for a ledger.
 *
 * <p>Invariants enforced: {@code 1 <= ackQuorum <= writeQuorum <= ensembleSize}. Defaults per role
 * (see DESIGN.md §6):
 * <ul>
 *   <li>WAL: 3/3/2 — recovery source, all-replica write, majority ack.</li>
 *   <li>MANIFEST: 3/3/2 — truth of LSM state, same as WAL.</li>
 *   <li>SSTABLE: 3/2/2 — durable but read-optimized, replaceable via re-compaction.</li>
 *   <li>SYRUP: 3/2/2 — tunable for throughput; raise Qa for stronger data durability.</li>
 * </ul>
 *
 * @param ensembleSize number of bookies across which entries are striped (E)
 * @param writeQuorum  number of replicas each entry is written to (Qw)
 * @param ackQuorum    number of acks required before an append is confirmed (Qa)
 */
public record QuorumConfig(int ensembleSize, int writeQuorum, int ackQuorum) {

    public QuorumConfig {
        if (ackQuorum < 1 || writeQuorum < ackQuorum || ensembleSize < writeQuorum) {
            throw new IllegalArgumentException(
                    "Require 1 <= ackQuorum <= writeQuorum <= ensembleSize, got E=" + ensembleSize
                            + " Qw=" + writeQuorum + " Qa=" + ackQuorum);
        }
    }

    public static QuorumConfig defaultFor(LedgerRole role) {
        return switch (role) {
            case WAL, MANIFEST -> new QuorumConfig(3, 3, 2);
            case SSTABLE, SYRUP -> new QuorumConfig(3, 2, 2);
        };
    }

    /** The full default table, keyed by role. */
    public static Map<LedgerRole, QuorumConfig> defaults() {
        return Map.of(
                LedgerRole.WAL, defaultFor(LedgerRole.WAL),
                LedgerRole.MANIFEST, defaultFor(LedgerRole.MANIFEST),
                LedgerRole.SSTABLE, defaultFor(LedgerRole.SSTABLE),
                LedgerRole.SYRUP, defaultFor(LedgerRole.SYRUP));
    }
}
