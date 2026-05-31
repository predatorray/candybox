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
        return new SSTableMeta(id, level, CandyKey.of("a"), CandyKey.of("z"), 10, 1024, Set.of(id + 1000));
    }

    @Test
    void applyAdvancesInMemoryStateAndStampsOwnerToken() {
        Manifest m = Manifest.createNew(store, cfg, 1L);
        m.apply(ManifestEdit.flush(table(100, 0), Set.of(7L, 8L), 42L));

        assertThat(m.current().level0()).extracting(SSTableMeta::ledgerId).containsExactly(100L);
        assertThat(m.current().liveSyrups()).contains(7L, 8L);
        assertThat(m.current().walLedgerId()).isEqualTo(42L);
    }

    @Test
    void editsSerializeAndReplayRoundTripIncludingToken() {
        ManifestEdit edit = ManifestEdit.builder()
                .addedTables(List.of(table(1, 0), table(2, 1)))
                .removedTableLedgerIds(Set.of(99L))
                .addedSyrups(Set.of(7L))
                .removedSyrups(Set.of(8L))
                .newWalLedgerId(5L)
                .ownerFencingToken(3L)
                .build();
        byte[] bytes = ManifestSerializer.serialize(edit);
        assertThat(ManifestSerializer.deserialize(bytes)).isEqualTo(edit);
    }

    @Test
    void multipartUploadsRoundTripThroughTheManifest() {
        java.util.Map<Integer, me.predatorray.candybox.common.Part> initialParts = new java.util.LinkedHashMap<>();
        MultipartUploadState created = new MultipartUploadState("upl-1", "obj/key", "image/png",
                java.util.Map.of("k", "v"), 12345L, initialParts);
        me.predatorray.candybox.common.Part part42 = new me.predatorray.candybox.common.Part(
                5L << 20, 1 << 20, 0xabcd1234,
                List.of(new me.predatorray.candybox.common.SegmentRef(77, 0, 4)));

        ManifestEdit createEdit = ManifestEdit.builder().addUpload(created).build();
        byte[] cb = ManifestSerializer.serialize(createEdit);
        assertThat(ManifestSerializer.deserialize(cb)).isEqualTo(createEdit);

        ManifestEdit partEdit = ManifestEdit.builder().addPartUpsert("upl-1", 42, part42).build();
        byte[] pb = ManifestSerializer.serialize(partEdit);
        assertThat(ManifestSerializer.deserialize(pb)).isEqualTo(partEdit);

        ManifestEdit dropEdit = ManifestEdit.builder().removedUploads(Set.of("upl-1")).build();
        byte[] db = ManifestSerializer.serialize(dropEdit);
        assertThat(ManifestSerializer.deserialize(db)).isEqualTo(dropEdit);

        // Apply Create + UploadPart and verify the in-memory state shows the part recorded.
        Manifest m = Manifest.createNew(store, cfg, 1L);
        m.apply(createEdit);
        m.apply(partEdit);
        assertThat(m.current().multipartUploads()).containsKey("upl-1");
        assertThat(m.current().multipartUploads().get("upl-1").parts()).containsKey(42);
        assertThat(m.current().multipartReferencedSyrups()).contains(77L);

        // Recover into a new manifest ledger and confirm the in-flight upload survived handover.
        long prior = m.ledgerId();
        m.close();
        Manifest recovered = Manifest.recover(store, cfg, prior, 2L);
        assertThat(recovered.current().multipartUploads()).containsKey("upl-1");
        assertThat(recovered.current().multipartUploads().get("upl-1").parts()).containsKey(42);
        recovered.close();
    }

    @Test
    void recoverReplaysPriorManifestIntoFreshLedger() {
        Manifest a = Manifest.createNew(store, cfg, 1L);
        a.apply(ManifestEdit.flush(table(100, 0), Set.of(7L), 9L));
        a.apply(ManifestEdit.builder().addedTables(List.of(table(101, 0))).build());
        long priorLedger = a.ledgerId();

        Manifest b = Manifest.recover(store, cfg, priorLedger, 2L);
        assertThat(b.current().tables()).extracting(SSTableMeta::ledgerId)
                .containsExactlyInAnyOrder(100L, 101L);
        assertThat(b.current().walLedgerId()).isEqualTo(9L);
        assertThat(b.current().liveSyrups()).contains(7L);
        // Fresh ledger is distinct from the (now fenced) prior one.
        assertThat(b.ledgerId()).isNotEqualTo(priorLedger);
    }

    @Test
    void fencedOwnerCannotCommitFurtherEdits() {
        Manifest owner = Manifest.createNew(store, cfg, 1L);
        owner.apply(ManifestEdit.flush(table(100, 0), Set.of(), 9L));

        // A new owner recover-opens (fences) the manifest ledger.
        store.recoverOpen(owner.ledgerId());

        assertThatThrownBy(() -> owner.apply(ManifestEdit.builder()
                .addedTables(List.of(table(200, 0))).build()))
                .isInstanceOf(FencedException.class);
    }

    @Test
    void recoverRejectsAStaleOwnerToken() {
        Manifest a = Manifest.createNew(store, cfg, 5L);
        a.apply(ManifestEdit.flush(table(100, 0), Set.of(), 9L));
        long priorLedger = a.ledgerId();

        // A node whose lease token (3) is below the committed max (5) must not take over.
        assertThatThrownBy(() -> Manifest.recover(store, cfg, priorLedger, 3L))
                .isInstanceOf(FencedException.class);
    }

    @Test
    void applyRejectsAnEditCarryingAStaleToken() {
        Manifest owner = Manifest.createNew(store, cfg, 5L);
        // An edit explicitly authored with a lower token (e.g. a zombie compactor) is rejected.
        assertThatThrownBy(() -> owner.apply(ManifestEdit.builder()
                .addedTables(List.of(table(100, 0))).ownerFencingToken(4L).build()))
                .isInstanceOf(FencedException.class);
    }
}
