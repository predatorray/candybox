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
    private final long leaseRenewIntervalMillis;
    private final long routerCacheTtlMillis;
    private final long compactionIntervalMillis;
    private final long maxClockSkewMillis;
    private final long tombstoneGcGraceMillis;
    private final long ledgerGcGraceMillis;
    private final int l0CompactionTrigger;
    private final int l0StallThreshold;
    private final long multipartMinPartBytes;
    private final int multipartMaxParts;
    private final long multipartUploadTtlMillis;
    private final int multipartMaxConcurrentUploadsPerBox;
    private final int partitionsPerBoxDefault;
    private final long balancerIntervalMillis;
    private final int balancerMaxMovesPerRound;

    private CandyboxConfig(Builder b) {
        this.sizeLimits = b.sizeLimits;
        this.quorums = new EnumMap<>(b.quorums);
        this.bloomBitsPerKey = b.bloomBitsPerKey;
        this.memtableFlushThresholdBytes = b.memtableFlushThresholdBytes;
        this.syrupRolloverBytes = b.syrupRolloverBytes;
        this.maxFrameSizeBytes = b.maxFrameSizeBytes;
        this.ownershipLeaseTtlMillis = b.ownershipLeaseTtlMillis;
        this.leaseRenewIntervalMillis = b.leaseRenewIntervalMillis;
        this.routerCacheTtlMillis = b.routerCacheTtlMillis;
        this.compactionIntervalMillis = b.compactionIntervalMillis;
        this.maxClockSkewMillis = b.maxClockSkewMillis;
        this.tombstoneGcGraceMillis = b.tombstoneGcGraceMillis;
        this.ledgerGcGraceMillis = b.ledgerGcGraceMillis;
        this.l0CompactionTrigger = b.l0CompactionTrigger;
        this.l0StallThreshold = b.l0StallThreshold;
        this.multipartMinPartBytes = b.multipartMinPartBytes;
        this.multipartMaxParts = b.multipartMaxParts;
        this.multipartUploadTtlMillis = b.multipartUploadTtlMillis;
        this.multipartMaxConcurrentUploadsPerBox = b.multipartMaxConcurrentUploadsPerBox;
        this.partitionsPerBoxDefault = b.partitionsPerBoxDefault;
        this.balancerIntervalMillis = b.balancerIntervalMillis;
        this.balancerMaxMovesPerRound = b.balancerMaxMovesPerRound;
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

    /** How often an owner renews its Box lease. {@code 0} disables the background heartbeat. */
    public long leaseRenewIntervalMillis() {
        return leaseRenewIntervalMillis;
    }

    /** How long the client caches a Box→owner routing entry before re-resolving. */
    public long routerCacheTtlMillis() {
        return routerCacheTtlMillis;
    }

    /** How often a node runs background compaction over its owned Boxes. {@code 0} disables it. */
    public long compactionIntervalMillis() {
        return compactionIntervalMillis;
    }

    public long maxClockSkewMillis() {
        return maxClockSkewMillis;
    }

    public long tombstoneGcGraceMillis() {
        return tombstoneGcGraceMillis;
    }

    /** Grace period before an obsoleted ledger (compaction input, dead Syrup) is physically deleted. */
    public long ledgerGcGraceMillis() {
        return ledgerGcGraceMillis;
    }

    /** Number of L0 SSTables that triggers a compaction. */
    public int l0CompactionTrigger() {
        return l0CompactionTrigger;
    }

    /** Number of L0 SSTables at which writes are rejected with {@code BUSY}. */
    public int l0StallThreshold() {
        return l0StallThreshold;
    }

    /** Minimum size of every part of a multipart upload except the last (S3-style). */
    public long multipartMinPartBytes() {
        return multipartMinPartBytes;
    }

    /** Maximum number of parts in a single multipart upload (S3 caps at 10,000). */
    public int multipartMaxParts() {
        return multipartMaxParts;
    }

    /** How long a pending multipart upload may remain before the background sweeper aborts it. */
    public long multipartUploadTtlMillis() {
        return multipartUploadTtlMillis;
    }

    /** Hard cap on the number of in-flight multipart uploads per Box. */
    public int multipartMaxConcurrentUploadsPerBox() {
        return multipartMaxConcurrentUploadsPerBox;
    }

    /** Partition count given to a new Box when the creator does not specify one. */
    public int partitionsPerBoxDefault() {
        return partitionsPerBoxDefault;
    }

    /** How often a node runs a partition-balancing round. {@code 0} disables the balancer. */
    public long balancerIntervalMillis() {
        return balancerIntervalMillis;
    }

    /** Max partitions moved away from live owners per balancing round (failover is not limited). */
    public int balancerMaxMovesPerRound() {
        return balancerMaxMovesPerRound;
    }

    public static final class Builder {
        private SizeLimits sizeLimits = SizeLimits.defaults();
        private Map<LedgerRole, QuorumConfig> quorums = new EnumMap<>(QuorumConfig.defaults());
        private int bloomBitsPerKey = 10;
        private long memtableFlushThresholdBytes = 4L << 20;   // 4 MiB
        private long syrupRolloverBytes = 1L << 30;            // 1 GiB
        private int maxFrameSizeBytes = 16 << 20;              // 16 MiB protocol cap
        private long ownershipLeaseTtlMillis = 10_000L;        // 10s lease
        private long leaseRenewIntervalMillis = 3_000L;        // renew well within the TTL; 0 disables
        private long routerCacheTtlMillis = 5_000L;            // client Box->owner cache TTL
        private long compactionIntervalMillis = 0L;            // background compaction; 0 disables
        private long maxClockSkewMillis = 300_000L;            // 5 min HLC skew bound
        private long tombstoneGcGraceMillis = 24L * 3600 * 1000; // 24h late-write window
        private long ledgerGcGraceMillis = 300_000L;           // 5 min before deleting obsolete ledgers
        private int l0CompactionTrigger = 4;
        private int l0StallThreshold = 12;
        private long multipartMinPartBytes = 5L << 20;                  // 5 MiB (S3 parity)
        private int multipartMaxParts = 10_000;                         // S3 cap
        private long multipartUploadTtlMillis = 7L * 24 * 3600 * 1000;  // 7 days
        private int multipartMaxConcurrentUploadsPerBox = 10_000;       // defensive ceiling
        private int partitionsPerBoxDefault = 8;                        // write spread vs. per-engine cost
        private long balancerIntervalMillis = 0L;                       // balancing round; 0 disables
        private int balancerMaxMovesPerRound = 4;                       // migration rate limit

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

        public Builder leaseRenewIntervalMillis(long v) {
            this.leaseRenewIntervalMillis = v;
            return this;
        }

        public Builder routerCacheTtlMillis(long v) {
            this.routerCacheTtlMillis = v;
            return this;
        }

        public Builder compactionIntervalMillis(long v) {
            this.compactionIntervalMillis = v;
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

        public Builder ledgerGcGraceMillis(long v) {
            this.ledgerGcGraceMillis = v;
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

        public Builder multipartMinPartBytes(long v) {
            this.multipartMinPartBytes = v;
            return this;
        }

        public Builder multipartMaxParts(int v) {
            this.multipartMaxParts = v;
            return this;
        }

        public Builder multipartUploadTtlMillis(long v) {
            this.multipartUploadTtlMillis = v;
            return this;
        }

        public Builder multipartMaxConcurrentUploadsPerBox(int v) {
            this.multipartMaxConcurrentUploadsPerBox = v;
            return this;
        }

        public Builder partitionsPerBoxDefault(int v) {
            this.partitionsPerBoxDefault = v;
            return this;
        }

        public Builder balancerIntervalMillis(long v) {
            this.balancerIntervalMillis = v;
            return this;
        }

        public Builder balancerMaxMovesPerRound(int v) {
            this.balancerMaxMovesPerRound = v;
            return this;
        }

        public CandyboxConfig build() {
            if (l0StallThreshold < l0CompactionTrigger) {
                throw new IllegalArgumentException("l0StallThreshold must be >= l0CompactionTrigger");
            }
            if (multipartMinPartBytes < 0) {
                throw new IllegalArgumentException("multipartMinPartBytes must be non-negative");
            }
            if (multipartMaxParts < 1) {
                throw new IllegalArgumentException("multipartMaxParts must be positive");
            }
            if (partitionsPerBoxDefault < 1) {
                throw new IllegalArgumentException("partitionsPerBoxDefault must be positive");
            }
            if (balancerMaxMovesPerRound < 1) {
                throw new IllegalArgumentException("balancerMaxMovesPerRound must be positive");
            }
            return new CandyboxConfig(this);
        }
    }
}
