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
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.config.LedgerRole;
import org.apache.bookkeeper.client.BookKeeper;
import org.junit.jupiter.api.Test;

class BookKeeperLedgerStoreExternalClientIT {

    @Test
    void closingStoreLeavesExternallyManagedClientUsable() throws Exception {
        byte[] password = "candybox".getBytes(StandardCharsets.UTF_8);
        try (EmbeddedBookKeeper embedded = new EmbeddedBookKeeper(3)) {
            var conf = embedded.clientConfiguration();
            try (BookKeeper client = new BookKeeper(conf)) {
                try (var store = new BookKeeperLedgerStore(client, conf, password)) {
                    try (var ledger = store.createLedger(LedgerConfig.forRole(LedgerRole.SSTABLE))) {
                        assertThat(ledger.append("entry".getBytes(StandardCharsets.UTF_8))).isZero();
                    }
                }

                try (var ledger = client.createLedger(3, 2, 2, BookKeeper.DigestType.CRC32C, password)) {
                    assertThat(ledger.addEntry("still open".getBytes(StandardCharsets.UTF_8))).isZero();
                }
            }
        }
    }
}
