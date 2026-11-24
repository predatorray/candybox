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
package me.predatorray.candybox.lsm;

import static me.predatorray.candybox.lsm.TestData.hlc;
import static me.predatorray.candybox.lsm.TestData.putMutation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.lsm.manifest.ManifestEdit;
import me.predatorray.candybox.lsm.manifest.ManifestSerializer;
import me.predatorray.candybox.lsm.manifest.MultipartUploadState;
import me.predatorray.candybox.lsm.sstable.SSTableReader;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;
import me.predatorray.candybox.lsm.wal.WalEntry;
import me.predatorray.candybox.lsm.wal.WalEntrySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FormatCompatibilityTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final LedgerConfig config = LedgerConfig.forRole(LedgerRole.SSTABLE);

    private static List<Mutation> mutations() {
        return IntStream.range(0, 20)
                .mapToObj(i -> putMutation(String.format("key-%03d", i), hlc(i + 1, 0, 1)))
                .toList();
    }

    @Test
    void readsV1SstableThroughPointLookupsAndFullScan() {
        long ledgerId = copyTableWithFooter(footer -> {
            // serializeFooter ends with the v2 range-del presence flag; no tombstones means false.
            assertThat(footer[4]).isEqualTo((byte) 2);
            assertThat(footer[footer.length - 1]).isZero();
            byte[] v1 = Arrays.copyOf(footer, footer.length - 1);
            v1[4] = 1;
            return v1;
        });

        try (SSTableReader reader = new SSTableReader(store, ledgerId)) {
            for (Mutation mutation : mutations()) {
                assertThat(reader.get(mutation.key())).contains(mutation.locator());
            }
            List<Mutation> scanned = new ArrayList<>();
            reader.scan(null).forEachRemaining(scanned::add);
            assertThat(scanned).containsExactlyElementsOf(mutations());
            assertThat(reader.rangeTombstones()).isEmpty();
        }
    }

    @Test
    void rejectsBadSstableFooterMagic() {
        long ledgerId = copyTableWithFooter(footer -> {
            footer[0] ^= 1;
            return footer;
        });
        assertThatThrownBy(() -> new SSTableReader(store, ledgerId))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("footer magic")
                .hasMessageContaining("ledger " + ledgerId);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3})
    void rejectsUnknownSstableVersion(int version) {
        long ledgerId = copyTableWithFooter(footer -> {
            footer[4] = (byte) version;
            return footer;
        });
        assertThatThrownBy(() -> new SSTableReader(store, ledgerId))
                .isInstanceOf(SerializationException.class)
                .hasMessage("Unsupported SSTable format version " + version);
    }

    private long copyTableWithFooter(UnaryOperator<byte[]> changeFooter) {
        long originalId = new SSTableWriter(store, 10, 256)
                .write(config, 0, mutations().iterator()).ledgerId();
        try (var original = store.openLedger(originalId);
             var copy = store.createLedger(config)) {
            List<LedgerEntry> entries = original.readRange(0, original.lastAddConfirmed());
            // Copy every earlier entry unchanged so data/index/bloom entry ids remain valid.
            for (int i = 0; i < entries.size() - 1; i++) {
                copy.append(entries.get(i).data());
            }
            byte[] footer = entries.get(entries.size() - 1).data().clone();
            copy.append(changeFooter.apply(footer));
            return copy.ledgerId();
        }
    }

    @Test
    void readsV2ManifestWithoutNewerOptionalFields() {
        Part part = new Part(25, 1024, 1234, List.of(new SegmentRef(80, 0, 1)));
        MultipartUploadState upload = new MultipartUploadState("upload-1", "object-key",
                "text/plain", Map.of("source", "legacy"), 12345, Map.of(1, part));
        // Zero tables avoids the v5-only fields embedded in each current table record.
        ManifestEdit edit = ManifestEdit.builder()
                .newWalLedgerId(300L).ownerFencingToken(400L)
                .addedUploads(List.of(upload))
                .addPartUpsert(upload.uploadId(), 2, part)
                .removedUploads(Set.of("completed-upload"))
                .build();
        byte[] current = ManifestSerializer.serialize(edit);
        // Remove two v6 pending-delete counts, the v4 maxHlc flag, then two v3 intent counts.
        assertThat(Arrays.copyOfRange(current, current.length - 5, current.length))
                .containsExactly(0, 0, 0, 0, 0);
        byte[] v2 = Arrays.copyOf(current, current.length - 5);
        v2[0] = 2;

        ManifestEdit decoded = ManifestSerializer.deserialize(v2);
        assertThat(decoded.newWalLedgerId()).isEqualTo(edit.newWalLedgerId());
        assertThat(decoded.ownerFencingToken()).isEqualTo(edit.ownerFencingToken());
        assertThat(decoded.addedUploads()).containsExactly(upload);
        assertThat(decoded.upsertParts()).containsExactlyElementsOf(edit.upsertParts());
        assertThat(decoded.removedUploads()).containsExactlyElementsOf(edit.removedUploads());
        assertThat(decoded.addedRenameIntents()).isEmpty();
        assertThat(decoded.removedRenameIntents()).isEmpty();
        assertThat(decoded.maxHlc()).isEqualTo(Hlc.MIN);
        assertThat(decoded.addedPendingDeletes()).isEmpty();
        assertThat(decoded.removedPendingDeletes()).isEmpty();
    }

    static IntStream unsupportedManifestVersions() {
        return IntStream.of(0, 1, ManifestSerializer.FORMAT_VERSION + 1);
    }

    @ParameterizedTest
    @MethodSource("unsupportedManifestVersions")
    void rejectsUnknownManifestVersion(int version) {
        byte[] encoded = ManifestSerializer.serialize(ManifestEdit.builder().newWalLedgerId(5L).build());
        encoded[0] = (byte) version;
        assertThatThrownBy(() -> ManifestSerializer.deserialize(encoded))
                .isInstanceOf(SerializationException.class)
                .hasMessage("Unsupported ManifestEdit version: " + version);
    }

    @Test
    void rejectsUnknownWalEntryKind() {
        byte[] encoded = WalEntrySerializer.serialize(WalEntry.of(putMutation("key", hlc(1, 0, 1))));
        encoded[0] = 0x7f;
        assertThatThrownBy(() -> WalEntrySerializer.deserialize(encoded))
                .isInstanceOf(SerializationException.class)
                .hasMessage("Unknown WAL entry kind: 127");
    }
}
