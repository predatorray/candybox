package me.predatorray.candybox.bookkeeper.fake;

import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.LedgerStoreContract;

/** Runs the shared {@link LedgerStoreContract} against the in-memory fake. */
class InMemoryLedgerStoreContractTest extends LedgerStoreContract {

    @Override
    protected LedgerStore newStore() {
        return new InMemoryLedgerStore();
    }
}
