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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class CandyboxConfigTest {

    @Test
    void defaultsExposeDocumentedTuningValues() {
        CandyboxConfig cfg = CandyboxConfig.defaults();

        assertThat(cfg.sizeLimits()).isEqualTo(SizeLimits.defaults());
        assertThat(cfg.bloomBitsPerKey()).isEqualTo(10);
        assertThat(cfg.memtableFlushThresholdBytes()).isEqualTo(4L << 20);
        assertThat(cfg.syrupRolloverBytes()).isEqualTo(1L << 30);
        assertThat(cfg.maxFrameSizeBytes()).isEqualTo(16 << 20);
        assertThat(cfg.ownershipLeaseTtlMillis()).isEqualTo(10_000L);
        assertThat(cfg.leaseRenewIntervalMillis()).isEqualTo(3_000L);
        assertThat(cfg.routerCacheTtlMillis()).isEqualTo(5_000L);
        assertThat(cfg.compactionIntervalMillis()).isZero();
        assertThat(cfg.maxClockSkewMillis()).isEqualTo(300_000L);
        assertThat(cfg.tombstoneGcGraceMillis()).isEqualTo(24L * 3600 * 1000);
        assertThat(cfg.ledgerGcGraceMillis()).isEqualTo(300_000L);
        assertThat(cfg.l0CompactionTrigger()).isEqualTo(4);
        assertThat(cfg.l0StallThreshold()).isEqualTo(12);
    }

    @Test
    void defaultsPopulateAQuorumForEveryRole() {
        CandyboxConfig cfg = CandyboxConfig.defaults();
        for (LedgerRole role : LedgerRole.values()) {
            assertThat(cfg.quorum(role)).isEqualTo(QuorumConfig.defaultFor(role));
        }
    }

    @Test
    void builderOverridesEveryFieldIndependently() {
        SizeLimits customLimits = new SizeLimits(2 << 20, 2 << 10, 1, 1 << 10, 50);
        QuorumConfig customWal = new QuorumConfig(5, 4, 3);
        CandyboxConfig cfg = CandyboxConfig.builder()
                .sizeLimits(customLimits)
                .quorum(LedgerRole.WAL, customWal)
                .bloomBitsPerKey(16)
                .memtableFlushThresholdBytes(123)
                .syrupRolloverBytes(456)
                .maxFrameSizeBytes(789)
                .ownershipLeaseTtlMillis(20_000)
                .leaseRenewIntervalMillis(0)
                .routerCacheTtlMillis(1_000)
                .compactionIntervalMillis(60_000)
                .maxClockSkewMillis(7)
                .tombstoneGcGraceMillis(8)
                .ledgerGcGraceMillis(9)
                .l0CompactionTrigger(2)
                .l0StallThreshold(2)
                .build();

        assertThat(cfg.sizeLimits()).isEqualTo(customLimits);
        assertThat(cfg.quorum(LedgerRole.WAL)).isEqualTo(customWal);
        // unset roles keep their defaults
        assertThat(cfg.quorum(LedgerRole.SYRUP)).isEqualTo(QuorumConfig.defaultFor(LedgerRole.SYRUP));
        assertThat(cfg.bloomBitsPerKey()).isEqualTo(16);
        assertThat(cfg.memtableFlushThresholdBytes()).isEqualTo(123);
        assertThat(cfg.syrupRolloverBytes()).isEqualTo(456);
        assertThat(cfg.maxFrameSizeBytes()).isEqualTo(789);
        assertThat(cfg.ownershipLeaseTtlMillis()).isEqualTo(20_000);
        assertThat(cfg.leaseRenewIntervalMillis()).isZero();
        assertThat(cfg.routerCacheTtlMillis()).isEqualTo(1_000);
        assertThat(cfg.compactionIntervalMillis()).isEqualTo(60_000);
        assertThat(cfg.maxClockSkewMillis()).isEqualTo(7);
        assertThat(cfg.tombstoneGcGraceMillis()).isEqualTo(8);
        assertThat(cfg.ledgerGcGraceMillis()).isEqualTo(9);
        assertThat(cfg.l0CompactionTrigger()).isEqualTo(2);
        assertThat(cfg.l0StallThreshold()).isEqualTo(2);
    }

    @Test
    void buildAllowsStallThresholdEqualToCompactionTrigger() {
        CandyboxConfig cfg = CandyboxConfig.builder()
                .l0CompactionTrigger(6)
                .l0StallThreshold(6)
                .build();
        assertThat(cfg.l0StallThreshold()).isEqualTo(cfg.l0CompactionTrigger());
    }

    @Test
    void buildRejectsStallThresholdBelowCompactionTrigger() {
        assertThatThrownBy(() -> CandyboxConfig.builder()
                .l0CompactionTrigger(8)
                .l0StallThreshold(4)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("l0StallThreshold");
    }

    @Test
    void builtConfigIsIsolatedFromLaterBuilderMutation() {
        CandyboxConfig.Builder builder = CandyboxConfig.builder()
                .quorum(LedgerRole.WAL, new QuorumConfig(3, 3, 2));
        CandyboxConfig cfg = builder.build();

        // Mutating the builder after build() must not change the already-built config.
        builder.quorum(LedgerRole.WAL, new QuorumConfig(9, 9, 9));
        assertThat(cfg.quorum(LedgerRole.WAL)).isEqualTo(new QuorumConfig(3, 3, 2));
    }

    @Test
    void multipartTuningDefaultsAndOverrides() {
        CandyboxConfig defaults = CandyboxConfig.defaults();
        assertThat(defaults.multipartMinPartBytes()).isEqualTo(5L << 20);
        assertThat(defaults.multipartMaxParts()).isEqualTo(10_000);
        assertThat(defaults.multipartUploadTtlMillis()).isPositive();
        assertThat(defaults.multipartMaxConcurrentUploadsPerBox()).isEqualTo(10_000);

        CandyboxConfig cfg = CandyboxConfig.builder()
                .multipartMinPartBytes(1024)
                .multipartMaxParts(50)
                .multipartUploadTtlMillis(123)
                .multipartMaxConcurrentUploadsPerBox(7)
                .build();
        assertThat(cfg.multipartMinPartBytes()).isEqualTo(1024);
        assertThat(cfg.multipartMaxParts()).isEqualTo(50);
        assertThat(cfg.multipartUploadTtlMillis()).isEqualTo(123);
        assertThat(cfg.multipartMaxConcurrentUploadsPerBox()).isEqualTo(7);
    }

    @Test
    void buildRejectsNegativeMultipartMinPartBytes() {
        assertThatThrownBy(() -> CandyboxConfig.builder().multipartMinPartBytes(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multipartMinPartBytes");
    }

    @Test
    void buildRejectsNonPositiveMultipartMaxParts() {
        assertThatThrownBy(() -> CandyboxConfig.builder().multipartMaxParts(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multipartMaxParts");
    }
}
