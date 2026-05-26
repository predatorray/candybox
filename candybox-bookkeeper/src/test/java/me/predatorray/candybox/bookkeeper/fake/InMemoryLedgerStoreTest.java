package me.predatorray.candybox.bookkeeper.fake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.config.QuorumConfig;
import me.predatorray.candybox.common.exception.StorageException;
import org.junit.jupiter.api.Test;

/** Adversarial behaviours specific to the fake: injected bookie loss / ack-quorum failures. */
class InMemoryLedgerStoreTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void appendFailsWhenAckQuorumCannotBeMet() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        WritableLedger w = store.createLedger(new LedgerConfig(new QuorumConfig(3, 3, 2)));
        w.append(bytes("ok-while-healthy"));

        store.setAvailableBookies(1); // below ack-quorum of 2
        assertThatThrownBy(() -> w.append(bytes("should-fail")))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("ack-quorum");

        store.setAvailableBookies(2); // back to healthy
        assertThat(w.append(bytes("ok-again"))).isEqualTo(1);
    }

    @Test
    void createFailsWhenEnsembleCannotBeFormed() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        store.setAvailableBookies(2);
        assertThatThrownBy(() -> store.createLedger(new LedgerConfig(new QuorumConfig(3, 3, 2))))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("ensemble");
    }
}
