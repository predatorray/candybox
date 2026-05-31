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

import java.util.Map;
import org.junit.jupiter.api.Test;

class QuorumConfigTest {

    @Test
    void defaultForWalAndManifestIsThreeThreeTwo() {
        assertThat(QuorumConfig.defaultFor(LedgerRole.WAL))
                .isEqualTo(new QuorumConfig(3, 3, 2));
        assertThat(QuorumConfig.defaultFor(LedgerRole.MANIFEST))
                .isEqualTo(new QuorumConfig(3, 3, 2));
    }

    @Test
    void defaultForSstableAndSyrupIsThreeTwoTwo() {
        assertThat(QuorumConfig.defaultFor(LedgerRole.SSTABLE))
                .isEqualTo(new QuorumConfig(3, 2, 2));
        assertThat(QuorumConfig.defaultFor(LedgerRole.SYRUP))
                .isEqualTo(new QuorumConfig(3, 2, 2));
    }

    @Test
    void defaultsTableCoversEveryRole() {
        Map<LedgerRole, QuorumConfig> table = QuorumConfig.defaults();
        assertThat(table).hasSize(LedgerRole.values().length);
        for (LedgerRole role : LedgerRole.values()) {
            assertThat(table.get(role)).isEqualTo(QuorumConfig.defaultFor(role));
        }
    }

    @Test
    void acceptsBoundaryWhereAllEqual() {
        QuorumConfig single = new QuorumConfig(1, 1, 1);
        assertThat(single.ensembleSize()).isEqualTo(1);
        assertThat(single.writeQuorum()).isEqualTo(1);
        assertThat(single.ackQuorum()).isEqualTo(1);
    }

    @Test
    void rejectsAckQuorumBelowOne() {
        assertThatThrownBy(() -> new QuorumConfig(3, 3, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ackQuorum");
    }

    @Test
    void rejectsWriteQuorumBelowAckQuorum() {
        assertThatThrownBy(() -> new QuorumConfig(3, 1, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsEnsembleBelowWriteQuorum() {
        assertThatThrownBy(() -> new QuorumConfig(2, 3, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
