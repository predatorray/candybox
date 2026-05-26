package me.predatorray.candybox.lsm.manifest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import org.junit.jupiter.api.Test;

class ManifestTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final LedgerConfig cfg = LedgerConfig.forRole(LedgerRole.MANIFEST);

    private static SSTableMeta table(long id, int level) {
        return new SSTableMeta(id, level, CandyKey.of("a"), CandyKey.of("z"), 10);
    }

    @Test
    void applyAdvancesInMemoryState() {
        Manifest m = Manifest.createNew(store, cfg);
        m.apply(ManifestEdit.flush(table(100, 0), Set.of(7L, 8L), 42L));

        assertThat(m.current().level0()).extracting(SSTableMeta::ledgerId).containsExactly(100L);
        assertThat(m.current().liveSyrups()).contains(7L, 8L);
        assertThat(m.current().walLedgerId()).isEqualTo(42L);
    }

    @Test
    void editsSerializeAndReplayRoundTrip() {
        ManifestEdit edit = new ManifestEdit(List.of(table(1, 0), table(2, 1)),
                Set.of(99L), Set.of(7L), Set.of(8L), 5L);
        byte[] bytes = ManifestSerializer.serialize(edit);
        assertThat(ManifestSerializer.deserialize(bytes)).isEqualTo(edit);
    }

    @Test
    void recoverReplaysPriorManifestIntoFreshLedger() {
        Manifest a = Manifest.createNew(store, cfg);
        a.apply(ManifestEdit.flush(table(100, 0), Set.of(7L), 9L));
        a.apply(ManifestEdit.builder().addedTables(List.of(table(101, 0))).build());
        long priorLedger = a.ledgerId();

        Manifest b = Manifest.recover(store, cfg, priorLedger);
        assertThat(b.current().tables()).extracting(SSTableMeta::ledgerId)
                .containsExactlyInAnyOrder(100L, 101L);
        assertThat(b.current().walLedgerId()).isEqualTo(9L);
        assertThat(b.current().liveSyrups()).contains(7L);
        // Fresh ledger is distinct from the (now fenced) prior one.
        assertThat(b.ledgerId()).isNotEqualTo(priorLedger);
    }

    @Test
    void fencedOwnerCannotCommitFurtherEdits() {
        Manifest owner = Manifest.createNew(store, cfg);
        owner.apply(ManifestEdit.flush(table(100, 0), Set.of(), 9L));

        // A new owner recover-opens (fences) the manifest ledger.
        store.recoverOpen(owner.ledgerId());

        assertThatThrownBy(() -> owner.apply(ManifestEdit.builder()
                .addedTables(List.of(table(200, 0))).build()))
                .isInstanceOf(FencedException.class);
    }
}
