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
