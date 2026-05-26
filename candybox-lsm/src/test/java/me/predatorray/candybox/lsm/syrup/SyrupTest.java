package me.predatorray.candybox.lsm.syrup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Random;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.StorageException;
import org.junit.jupiter.api.Test;

class SyrupTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();

    private CandyboxConfig smallChunkConfig() {
        SizeLimits limits = new SizeLimits(16, SizeLimits.DEFAULT_MAX_KEY_BYTES,
                SizeLimits.DEFAULT_MAX_METADATA_BYTES, SizeLimits.DEFAULT_MAX_LOCATOR_BYTES, 0);
        return CandyboxConfig.builder().sizeLimits(limits).syrupRolloverBytes(40).build();
    }

    @Test
    void chunksLargeCandyAcrossMultipleSyrupsAndReassembles() {
        byte[] original = new byte[100];
        new Random(42).nextBytes(original);

        SyrupManager writer = new SyrupManager(store, smallChunkConfig(),
                LedgerConfig.forRole(LedgerRole.SYRUP));
        SyrupWriteResult result = writer.writeCandy(new ByteArrayInputStream(original));
        writer.close();

        assertThat(result.contentLength()).isEqualTo(100);
        // 100 bytes / 16-byte chunks = 7 chunks; ~2 chunks per 40-byte Syrup => several segments.
        assertThat(result.segments().size()).isGreaterThan(1);

        byte[] readBack = new SyrupReader(store).readAll(result.segments(), result.contentLength());
        assertThat(readBack).isEqualTo(original);
    }

    @Test
    void emptyCandyProducesNoSegments() {
        SyrupManager writer = new SyrupManager(store, smallChunkConfig(),
                LedgerConfig.forRole(LedgerRole.SYRUP));
        SyrupWriteResult result = writer.writeCandy(new ByteArrayInputStream(new byte[0]));
        writer.close();

        assertThat(result.contentLength()).isZero();
        assertThat(result.segments()).isEmpty();
        assertThat(new SyrupReader(store).readAll(result.segments(), 0)).isEmpty();
    }

    @Test
    void detectsCorruptedChunkViaPerChunkCrc() {
        // Hand-build a Syrup entry whose stored CRC does not match its payload.
        WritableLedger syrup = store.createLedger(LedgerConfig.forRole(LedgerRole.SYRUP));
        byte[] badEntry = new byte[]{0, 0, 0, 0, 'b', 'a', 'd'}; // crc=0 but payload "bad"
        long entryId = syrup.append(badEntry);
        syrup.close();

        List<SegmentRef> segments = List.of(new SegmentRef(syrup.ledgerId(), entryId, entryId));
        assertThatThrownBy(() -> new SyrupReader(store).readAll(segments, 3))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("CRC mismatch");
    }
}
