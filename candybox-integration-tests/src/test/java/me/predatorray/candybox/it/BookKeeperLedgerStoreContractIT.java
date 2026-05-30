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

import java.nio.charset.StandardCharsets;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.LedgerStoreContract;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Runs the shared {@link LedgerStoreContract} against the real BookKeeper-backed store on an embedded
 * cluster — the same suite the in-memory fake passes, proving the fake is a faithful stand-in.
 */
class BookKeeperLedgerStoreContractIT extends LedgerStoreContract {

    private static EmbeddedBookKeeper bookKeeper;

    @BeforeAll
    static void startCluster() {
        bookKeeper = new EmbeddedBookKeeper(3);
    }

    @AfterAll
    static void stopCluster() {
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    @Override
    protected LedgerStore newStore() {
        return new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                "candybox".getBytes(StandardCharsets.UTF_8));
    }
}
