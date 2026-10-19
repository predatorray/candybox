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
package me.predatorray.candybox.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Partitioning;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.transport.LoopbackTransport;
import me.predatorray.candybox.server.CandyboxNode;
import org.junit.jupiter.api.Test;

/**
 * Client-driven cross-partition rename away and back, then GC past the grace period: the Syrup the
 * renamed-back object points at must survive (issue #46). Runs on the in-memory fakes.
 */
class CrossPartitionRenameGcIT {

    private static final int PARTITIONS = 2;

    private static String keyIn(int partition, String prefix) {
        for (int i = 0; ; i++) {
            if (Partitioning.partitionOf(prefix + i, PARTITIONS) == partition) {
                return prefix + i;
            }
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void renameAwayAndBackThenGcKeepsTheObjectReadable() {
        ManualClock clock = new ManualClock(1_000_000);
        CandyboxConfig config = CandyboxConfig.builder()
                .syrupRolloverBytes(1)
                .l0CompactionTrigger(2)
                .ownershipLeaseTtlMillis(3_600_000)
                .leaseRenewIntervalMillis(0)
                .build();
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        BoxName box = BoxName.of("gc-box");
        String a = keyIn(0, "a");
        String b = keyIn(1, "b");
        try (CandyboxNode node = new CandyboxNode(1, config, store,
                new InMemoryCoordinationService(clock), clock);
             CandyboxClient client = new CandyboxClient(new LoopbackTransport(node.requestHandler()),
                     "x", 0)) {
            node.createBox(box, PARTITIONS);
            client.putCandy(box.value(), a, bytes("precious"), null, Map.of(), null);
            client.putCandy(box.value(), keyIn(0, "other"), bytes("x"), null, Map.of(), null);
            node.enginePartition(box, 0).flush();
            client.renameCandy(box.value(), a, b, null);
            node.enginePartition(box, 0).flush();
            node.compactOwnedBoxesOnce();
            client.renameCandy(box.value(), b, a, null);
            node.enginePartition(box, 1).flush();
            node.compactOwnedBoxesOnce();

            clock.advance(config.ledgerGcGraceMillis() + 1);
            node.collectGarbageOnce();
            assertThat(client.getCandy(box.value(), a)).isEqualTo(bytes("precious"));
        }
        store.close();
    }
}
