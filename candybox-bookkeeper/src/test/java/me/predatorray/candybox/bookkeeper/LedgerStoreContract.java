package me.predatorray.candybox.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.StorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The shared {@link LedgerStore} behavioural contract. Both the in-memory fake and the real
 * BookKeeper-backed store must pass this identical suite — that equivalence is what makes the fake a
 * trustworthy stand-in for the fencing/handover tests that never touch real BookKeeper.
 *
 * <p>Subclasses supply a fresh store via {@link #newStore()} and the quorum to use via
 * {@link #config()} (the real integration test overrides it to match its embedded bookie count).
 */
public abstract class LedgerStoreContract {

    private LedgerStore store;

    /** Produces a fresh, empty store for one test. */
    protected abstract LedgerStore newStore();

    /** The ledger config (quorum) the contract uses. Default: the SSTABLE role's 3/2/2. */
    protected LedgerConfig config() {
        return LedgerConfig.forRole(LedgerRole.SSTABLE);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @BeforeEach
    void setUpStore() {
        store = newStore();
    }

    @AfterEach
    void tearDownStore() {
        if (store != null) {
            store.close();
        }
    }

    @Test
    void createAppendAndReadRoundTrip() {
        WritableLedger w = store.createLedger(config());
        long e0 = w.append(bytes("alpha"));
        long e1 = w.append(bytes("beta"));
        assertThat(e0).isEqualTo(0);
        assertThat(e1).isEqualTo(1);
        assertThat(w.lastAddConfirmed()).isEqualTo(1);

        w.close();

        ReadableLedger r = store.openLedger(w.ledgerId());
        assertThat(r.lastAddConfirmed()).isEqualTo(1);
        assertThat(new String(r.read(0).data(), StandardCharsets.UTF_8)).isEqualTo("alpha");
        List<LedgerEntry> all = r.readRange(0, 1);
        assertThat(all).extracting(e -> new String(e.data(), StandardCharsets.UTF_8))
                .containsExactly("alpha", "beta");
    }

    @Test
    void openLedgerDoesNotFenceTheWriter() {
        WritableLedger w = store.createLedger(config());
        w.append(bytes("a"));
        w.append(bytes("b"));

        // Passive read-only open must not fence: the writer can keep appending.
        store.openLedger(w.ledgerId());
        long e2 = w.append(bytes("c"));
        assertThat(e2).isEqualTo(2);

        w.close();
        ReadableLedger r = store.openLedger(w.ledgerId());
        assertThat(r.readRange(0, 2)).hasSize(3);
    }

    @Test
    void recoverOpenSealsAndFencesPriorWriter() {
        WritableLedger w = store.createLedger(config());
        w.append(bytes("one"));
        w.append(bytes("two"));
        // Deliberately do NOT close: simulate a still-live (about-to-be-zombie) writer.

        ReadableLedger recovered = store.recoverOpen(w.ledgerId());
        assertThat(recovered.isSealed()).isTrue();
        assertThat(recovered.lastAddConfirmed()).isEqualTo(1);
        assertThat(recovered.readRange(0, 1))
                .extracting(e -> new String(e.data(), StandardCharsets.UTF_8))
                .containsExactly("one", "two");

        // The fenced prior writer can no longer append — the zombie-owner defense.
        assertThatThrownBy(() -> w.append(bytes("three"))).isInstanceOf(FencedException.class);
    }

    @Test
    void closedLedgerRejectsFurtherAppends() {
        WritableLedger w = store.createLedger(config());
        w.append(bytes("x"));
        w.close();
        assertThatThrownBy(() -> w.append(bytes("y"))).isInstanceOf(FencedException.class);
    }

    @Test
    void readingOutOfRangeThrows() {
        WritableLedger w = store.createLedger(config());
        w.append(bytes("only"));
        w.close();
        ReadableLedger r = store.openLedger(w.ledgerId());
        assertThatThrownBy(() -> r.read(5)).isInstanceOf(StorageException.class);
    }

    @Test
    void deleteRemovesLedger() {
        WritableLedger w = store.createLedger(config());
        w.append(bytes("doomed"));
        w.close();
        long id = w.ledgerId();

        assertThat(store.listLedgers()).contains(id);
        store.deleteLedger(id);
        assertThat(store.listLedgers()).doesNotContain(id);
        assertThatThrownBy(() -> store.openLedger(id)).isInstanceOf(LedgerNotFoundException.class);
    }

    @Test
    void customMetadataIsPreserved() {
        LedgerConfig cfg = new LedgerConfig(config().quorum(),
                java.util.Map.of("role", bytes("SSTABLE"), "box", bytes("my-box")));
        WritableLedger w = store.createLedger(cfg);
        w.append(bytes("z"));
        w.close();

        ReadableLedger r = store.openLedger(w.ledgerId());
        assertThat(new String(r.customMetadata().get("role"), StandardCharsets.UTF_8))
                .isEqualTo("SSTABLE");
    }
}
