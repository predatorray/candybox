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
package me.predatorray.candybox.bookkeeper.fake;

import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.LedgerStoreContract;

/** Runs the shared {@link LedgerStoreContract} against the in-memory fake. */
class InMemoryLedgerStoreContractTest extends LedgerStoreContract {

    @Override
    protected int availableBookies() {
        return 3;
    }

    @Override
    protected LedgerStore newStore() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        store.setAvailableBookies(availableBookies());
        return store;
    }
}
