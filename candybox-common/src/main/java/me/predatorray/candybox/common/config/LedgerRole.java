package me.predatorray.candybox.common.config;

/**
 * The distinct roles a BookKeeper ledger plays in Candybox. Each role has its own durability/quorum
 * defaults (see {@link QuorumConfig}) and lifecycle rules; keeping them separate in code prevents,
 * say, a throughput-tuned Syrup ledger from being created with WAL durability by accident.
 */
public enum LedgerRole {
    /** Write-ahead log; recovery source, strongest durability. */
    WAL,
    /** Append-only LSM metadata (manifest); truth of LSM state. */
    MANIFEST,
    /** Immutable sorted run produced by flush/compaction. */
    SSTABLE,
    /** Raw Candy bytes, chunked across entries. */
    SYRUP
}
