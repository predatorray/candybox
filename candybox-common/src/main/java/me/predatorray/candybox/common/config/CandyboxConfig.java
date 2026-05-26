package me.predatorray.candybox.common.config;

import java.util.EnumMap;
import java.util.Map;

/**
 * The full, immutable Candybox tuning surface. Every value the prompt asks to "pick a reasonable
 * default, keep configurable, and document" lives here; rationale is in DESIGN.md §13.
 *
 * <p>Construct via {@link #builder()}; {@link #defaults()} returns the documented defaults.
 */
public final class CandyboxConfig {

    private final SizeLimits sizeLimits;
    private final Map<LedgerRole, QuorumConfig> quorums;
    private final int bloomBitsPerKey;
    private final long memtableFlushThresholdBytes;
    private final long syrupRolloverBytes;
    private final int maxFrameSizeBytes;
    private final long ownershipLeaseTtlMillis;
    private final long maxClockSkewMillis;
    private final long tombstoneGcGraceMillis;
    private final int l0CompactionTrigger;
    private final int l0StallThreshold;

    private CandyboxConfig(Builder b) {
        this.sizeLimits = b.sizeLimits;
        this.quorums = new EnumMap<>(b.quorums);
        this.bloomBitsPerKey = b.bloomBitsPerKey;
        this.memtableFlushThresholdBytes = b.memtableFlushThresholdBytes;
        this.syrupRolloverBytes = b.syrupRolloverBytes;
        this.maxFrameSizeBytes = b.maxFrameSizeBytes;
        this.ownershipLeaseTtlMillis = b.ownershipLeaseTtlMillis;
        this.maxClockSkewMillis = b.maxClockSkewMillis;
        this.tombstoneGcGraceMillis = b.tombstoneGcGraceMillis;
        this.l0CompactionTrigger = b.l0CompactionTrigger;
        this.l0StallThreshold = b.l0StallThreshold;
    }

    public static CandyboxConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public SizeLimits sizeLimits() {
        return sizeLimits;
    }

    public QuorumConfig quorum(LedgerRole role) {
        return quorums.get(role);
    }

    public int bloomBitsPerKey() {
        return bloomBitsPerKey;
    }

    public long memtableFlushThresholdBytes() {
        return memtableFlushThresholdBytes;
    }

    public long syrupRolloverBytes() {
        return syrupRolloverBytes;
    }

    public int maxFrameSizeBytes() {
        return maxFrameSizeBytes;
    }

    public long ownershipLeaseTtlMillis() {
        return ownershipLeaseTtlMillis;
    }

    public long maxClockSkewMillis() {
        return maxClockSkewMillis;
    }

    public long tombstoneGcGraceMillis() {
        return tombstoneGcGraceMillis;
    }

    /** Number of L0 SSTables that triggers a compaction. */
    public int l0CompactionTrigger() {
        return l0CompactionTrigger;
    }

    /** Number of L0 SSTables at which writes are rejected with {@code BUSY}. */
    public int l0StallThreshold() {
        return l0StallThreshold;
    }

    public static final class Builder {
        private SizeLimits sizeLimits = SizeLimits.defaults();
        private Map<LedgerRole, QuorumConfig> quorums = new EnumMap<>(QuorumConfig.defaults());
        private int bloomBitsPerKey = 10;
        private long memtableFlushThresholdBytes = 4L << 20;   // 4 MiB
        private long syrupRolloverBytes = 1L << 30;            // 1 GiB
        private int maxFrameSizeBytes = 16 << 20;              // 16 MiB protocol cap
        private long ownershipLeaseTtlMillis = 10_000L;        // 10s lease
        private long maxClockSkewMillis = 300_000L;            // 5 min HLC skew bound
        private long tombstoneGcGraceMillis = 24L * 3600 * 1000; // 24h late-write window
        private int l0CompactionTrigger = 4;
        private int l0StallThreshold = 12;

        public Builder sizeLimits(SizeLimits v) {
            this.sizeLimits = v;
            return this;
        }

        public Builder quorum(LedgerRole role, QuorumConfig v) {
            this.quorums.put(role, v);
            return this;
        }

        public Builder bloomBitsPerKey(int v) {
            this.bloomBitsPerKey = v;
            return this;
        }

        public Builder memtableFlushThresholdBytes(long v) {
            this.memtableFlushThresholdBytes = v;
            return this;
        }

        public Builder syrupRolloverBytes(long v) {
            this.syrupRolloverBytes = v;
            return this;
        }

        public Builder maxFrameSizeBytes(int v) {
            this.maxFrameSizeBytes = v;
            return this;
        }

        public Builder ownershipLeaseTtlMillis(long v) {
            this.ownershipLeaseTtlMillis = v;
            return this;
        }

        public Builder maxClockSkewMillis(long v) {
            this.maxClockSkewMillis = v;
            return this;
        }

        public Builder tombstoneGcGraceMillis(long v) {
            this.tombstoneGcGraceMillis = v;
            return this;
        }

        public Builder l0CompactionTrigger(int v) {
            this.l0CompactionTrigger = v;
            return this;
        }

        public Builder l0StallThreshold(int v) {
            this.l0StallThreshold = v;
            return this;
        }

        public CandyboxConfig build() {
            if (l0StallThreshold < l0CompactionTrigger) {
                throw new IllegalArgumentException("l0StallThreshold must be >= l0CompactionTrigger");
            }
            return new CandyboxConfig(this);
        }
    }
}
