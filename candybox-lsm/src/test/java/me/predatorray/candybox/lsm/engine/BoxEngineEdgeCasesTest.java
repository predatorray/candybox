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
package me.predatorray.candybox.lsm.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.auth.Grant;
import me.predatorray.candybox.common.auth.ObjectAcl;
import me.predatorray.candybox.common.auth.Operation;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.ValidationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Edge-case and validation coverage for {@link BoxEngine} that complements the happy-path
 * {@link BoxEngineTest}: argument validation across the multipart API, object-ACL reads/writes,
 * range-GET boundary resolution, prefix/range scan corners, and recovery that re-opens SSTables.
 */
class BoxEngineEdgeCasesTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final BoxName box = BoxName.of("my-box");
    private BoxEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
        store.close();
    }

    private BoxEngine newEngine() {
        return newEngine(CandyboxConfig.defaults());
    }

    private BoxEngine newEngine(CandyboxConfig cfg) {
        return BoxEngine.createNew(box, cfg, store, 1, new ManualClock(1000), 1L);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- put / metadata ---------------------------------------------------------------------

    @Test
    void putAcceptsNullUserMetadataAndExposesBoxName() {
        engine = newEngine();
        engine.putCandy(CandyKey.of("k"), bytes("v"), "text/plain", null, null);
        assertThat(engine.headCandy(CandyKey.of("k")).userMetadata()).isEmpty();
        assertThat(engine.box()).isEqualTo(box);
    }

    // ---- multipart: createMultipartUpload ----------------------------------------------------

    @Test
    void createMultipartUploadAcceptsNullMetadataAndCapsConcurrency() {
        CandyboxConfig cfg = CandyboxConfig.builder()
                .multipartMaxConcurrentUploadsPerBox(1)
                .build();
        engine = newEngine(cfg);
        String id = engine.createMultipartUpload(CandyKey.of("a"), null, null);
        assertThat(engine.multipartUpload(id)).isNotNull();
        assertThat(engine.listMultipartUploads()).hasSize(1);
        // The second concurrent upload exceeds the per-Box cap.
        assertThatThrownBy(() -> engine.createMultipartUpload(CandyKey.of("b"), null, Map.of()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Too many in-flight");
    }

    // ---- multipart: uploadPart ---------------------------------------------------------------

    @Test
    void uploadPartValidatesUploadIdAndPartNumber() {
        engine = newEngine();
        assertThatThrownBy(() -> engine.uploadPart("", 1, bytes("x")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("uploadId is required");
        String id = engine.createMultipartUpload(CandyKey.of("k"), null, Map.of());
        assertThatThrownBy(() -> engine.uploadPart(id, 0, bytes("x")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("partNumber");
        assertThatThrownBy(() -> engine.uploadPart("no-such-upload", 1, bytes("x")))
                .isInstanceOf(CandyNotFoundException.class);
    }

    @Test
    void reUploadingSamePartNumberSupersedesThePriorPart() {
        CandyboxConfig cfg = CandyboxConfig.builder().multipartMinPartBytes(1).build();
        engine = newEngine(cfg);
        String id = engine.createMultipartUpload(CandyKey.of("k"), null, Map.of());
        engine.uploadPart(id, 1, bytes("first-version"));
        BoxEngine.PartUploadResult second = engine.uploadPart(id, 1, bytes("second"));
        // The recorded part reflects the latest upload.
        assertThat(engine.multipartUpload(id).parts().get(1).partLength()).isEqualTo(6);
        engine.completeMultipartUpload(id, List.of(new BoxEngine.PartCompletion(1, second.crc32c())),
                null);
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("second"));
    }

    // ---- multipart: uploadPartCopy -----------------------------------------------------------

    @Test
    void uploadPartCopyValidatesArgumentsAndRange() {
        CandyboxConfig cfg = CandyboxConfig.builder().multipartMinPartBytes(1).build();
        engine = newEngine(cfg);
        engine.putCandy(CandyKey.of("src"), bytes("hello candybox"), null, Map.of(), null);
        String id = engine.createMultipartUpload(CandyKey.of("dst"), null, Map.of());

        assertThatThrownBy(() -> engine.uploadPartCopy("", 1, CandyKey.of("src"), 0, 1))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(() -> engine.uploadPartCopy(id, 0, CandyKey.of("src"), 0, 1))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("partNumber");
        assertThatThrownBy(() -> engine.uploadPartCopy("missing", 1, CandyKey.of("src"), 0, 1))
                .isInstanceOf(CandyNotFoundException.class);
        assertThatThrownBy(() -> engine.uploadPartCopy(id, 1, CandyKey.of("absent"), 0, 1))
                .isInstanceOf(CandyNotFoundException.class);
        // first byte beyond the object end is not satisfiable.
        assertThatThrownBy(() -> engine.uploadPartCopy(id, 1, CandyKey.of("src"), 100, 200))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("InvalidRange");
    }

    @Test
    void uploadPartCopyWithOpenEndedBoundsCopiesToObjectEnd() {
        CandyboxConfig cfg = CandyboxConfig.builder().multipartMinPartBytes(1).build();
        engine = newEngine(cfg);
        engine.putCandy(CandyKey.of("src"), bytes("hello candybox"), null, Map.of(), null);
        String id = engine.createMultipartUpload(CandyKey.of("dst"), null, Map.of());
        // firstByte < 0 resolves to 0; lastByte < 0 resolves to the object's last byte.
        BoxEngine.PartUploadResult copied = engine.uploadPartCopy(id, 1, CandyKey.of("src"), -1, -1);
        engine.completeMultipartUpload(id, List.of(new BoxEngine.PartCompletion(1, copied.crc32c())),
                null);
        assertThat(engine.getCandy(CandyKey.of("dst"))).isEqualTo(bytes("hello candybox"));
    }

    // ---- multipart: completeMultipartUpload / abort ------------------------------------------

    @Test
    void completeMultipartUploadValidatesItsArguments() {
        engine = newEngine();
        assertThatThrownBy(() -> engine.completeMultipartUpload("", List.of(), null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("uploadId is required");
        String id = engine.createMultipartUpload(CandyKey.of("k"), null, Map.of());
        assertThatThrownBy(() -> engine.completeMultipartUpload(id, List.of(), null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("at least one part");
        assertThatThrownBy(() -> engine.completeMultipartUpload("missing",
                List.of(new BoxEngine.PartCompletion(1, 0)), null))
                .isInstanceOf(CandyNotFoundException.class);
    }

    @Test
    void completeMultipartUploadRejectsNonAscendingAndUnknownParts() {
        CandyboxConfig cfg = CandyboxConfig.builder().multipartMinPartBytes(1).build();
        engine = newEngine(cfg);
        String id = engine.createMultipartUpload(CandyKey.of("k"), null, Map.of());
        BoxEngine.PartUploadResult p1 = engine.uploadPart(id, 1, bytes("aaaa"));
        BoxEngine.PartUploadResult p2 = engine.uploadPart(id, 2, bytes("bbbb"));
        // Non-ascending order.
        assertThatThrownBy(() -> engine.completeMultipartUpload(id, List.of(
                new BoxEngine.PartCompletion(2, p2.crc32c()),
                new BoxEngine.PartCompletion(1, p1.crc32c())), null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("ascending");
        // Reference to a part that was never uploaded.
        assertThatThrownBy(() -> engine.completeMultipartUpload(id, List.of(
                new BoxEngine.PartCompletion(9, 0)), null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unknown part");
    }

    @Test
    void completeMultipartUploadIsIdempotentUnderToken() {
        CandyboxConfig cfg = CandyboxConfig.builder().multipartMinPartBytes(1).build();
        engine = newEngine(cfg);
        String id = engine.createMultipartUpload(CandyKey.of("k"), null, Map.of());
        BoxEngine.PartUploadResult p1 = engine.uploadPart(id, 1, bytes("payload"));
        CandyMetadata first = engine.completeMultipartUpload(id,
                List.of(new BoxEngine.PartCompletion(1, p1.crc32c())), "tok-1");
        // The retry replays the cached result rather than failing on the now-removed upload.
        CandyMetadata retry = engine.completeMultipartUpload(id,
                List.of(new BoxEngine.PartCompletion(1, p1.crc32c())), "tok-1");
        assertThat(retry.hlc()).isEqualTo(first.hlc());
    }

    @Test
    void abortMultipartUploadRejectsBlankUploadId() {
        engine = newEngine();
        assertThatThrownBy(() -> engine.abortMultipartUpload(""))
                .isInstanceOf(ValidationException.class);
    }

    // ---- object ACLs -------------------------------------------------------------------------

    @Test
    void objectAclIsStoredOnPutAndReplacedBySetAcl() {
        engine = newEngine();
        Principal alice = Principal.user("alice");
        ObjectAcl owned = new ObjectAcl(alice.toString(),
                List.of(Grant.of(Grant.ALL_USERS, Operation.READ)));
        engine.putCandy(CandyKey.of("k"), new java.io.ByteArrayInputStream(bytes("v")), null,
                Map.of(), null, owned);
        assertThat(engine.getCandyAcl(CandyKey.of("k")).owner()).isEqualTo(alice.toString());

        // setCandyAcl rewrites the document in place (zero-copy locator rewrite) and survives a flush.
        ObjectAcl replaced = ObjectAcl.ownedBy(Principal.user("bob"));
        engine.setCandyAcl(CandyKey.of("k"), replaced);
        engine.flush();
        assertThat(engine.getCandyAcl(CandyKey.of("k")).owner())
                .isEqualTo(Principal.user("bob").toString());
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v")); // bytes intact
    }

    @Test
    void aclOperationsOnMissingKeyThrowNotFound() {
        engine = newEngine();
        assertThatThrownBy(() -> engine.getCandyAcl(CandyKey.of("ghost")))
                .isInstanceOf(CandyNotFoundException.class);
        assertThatThrownBy(() -> engine.setCandyAcl(CandyKey.of("ghost"), ObjectAcl.NONE))
                .isInstanceOf(CandyNotFoundException.class);
    }

    // ---- range GET boundary resolution -------------------------------------------------------

    @Test
    void rangeGetBoundaryResolution() {
        engine = newEngine();
        engine.putCandy(CandyKey.of("k"), bytes("hello candybox"), null, Map.of(), null); // 14 bytes

        // Missing key.
        assertThatThrownBy(() -> engine.getCandyRange(CandyKey.of("ghost"), 0, 1,
                new ByteArrayOutputStream())).isInstanceOf(CandyNotFoundException.class);
        // Neither bound supplied.
        assertThatThrownBy(() -> engine.getCandyRange(CandyKey.of("k"), -1, -1,
                new ByteArrayOutputStream())).isInstanceOf(IllegalArgumentException.class);
        // Non-positive suffix.
        assertThatThrownBy(() -> engine.getCandyRange(CandyKey.of("k"), -1, 0,
                new ByteArrayOutputStream())).isInstanceOf(IllegalArgumentException.class);
        // firstByte at/after end is unsatisfiable.
        assertThatThrownBy(() -> engine.getCandyRange(CandyKey.of("k"), 14, -1,
                new ByteArrayOutputStream())).isInstanceOf(IllegalArgumentException.class);

        // Suffix larger than the object clamps to the whole object.
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        BoxEngine.RangeReadResult whole = engine.getCandyRange(CandyKey.of("k"), -1, 999, all);
        assertThat(all.toByteArray()).isEqualTo(bytes("hello candybox"));
        assertThat(whole.firstByte()).isZero();
        assertThat(whole.lastByte()).isEqualTo(13);
    }

    @Test
    void rangeGetOnEmptyObjectIsUnsatisfiable() {
        engine = newEngine();
        engine.putCandy(CandyKey.of("empty"), new byte[0], null, Map.of(), null);
        assertThatThrownBy(() -> engine.getCandyRange(CandyKey.of("empty"), 0, 0,
                new ByteArrayOutputStream())).isInstanceOf(IllegalArgumentException.class);
    }

    // ---- scans -------------------------------------------------------------------------------

    @Test
    void deleteRangeByPrefixWithEmptyPrefixClearsTheBox() {
        engine = newEngine();
        engine.putCandy(CandyKey.of("a"), bytes("x"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("b"), bytes("x"), null, Map.of(), null);
        engine.deleteRangeByPrefix(""); // empty prefix => whole-keyspace tombstone
        assertThat(engine.listCandies(null, null, 100).entries()).isEmpty();
    }

    @Test
    void deleteRangeByPrefixWithHighBytePrefixHasUnboundedSuccessor() {
        engine = newEngine();
        // A prefix of 0xFF bytes has no lexicographic successor, so the upper bound is "unbounded".
        String highPrefix = new String(new byte[]{(byte) 0xFF}, StandardCharsets.UTF_8);
        engine.putCandy(CandyKey.of(highPrefix + "z"), bytes("x"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("a"), bytes("x"), null, Map.of(), null);
        engine.deleteRangeByPrefix(highPrefix);
        assertThat(engine.listCandies(null, null, 100).entries())
                .extracting(e -> e.key().value()).containsExactly("a");
    }

    @Test
    void forwardScanHonoursPrefixIntersectedWithExplicitWindow() {
        engine = newEngine();
        for (String k : new String[] {"p/a", "p/b", "p/c", "q/a"}) {
            engine.putCandy(CandyKey.of(k), bytes("x"), null, Map.of(), null);
        }
        engine.flush();
        // Prefix "p/" intersected with [p/b, p/c) yields only "p/b".
        ListResult res = engine.scanCandies(new ScanQuery("p/", CandyKey.of("p/b"),
                CandyKey.of("p/c"), null, ScanDirection.FORWARD, 100));
        assertThat(res.entries()).extracting(e -> e.key().value()).containsExactly("p/b");
    }

    @Test
    void reverseScanRespectsLowerAndUpperBounds() {
        engine = newEngine();
        for (String k : new String[] {"a", "b", "c", "d", "e"}) {
            engine.putCandy(CandyKey.of(k), bytes("x"), null, Map.of(), null);
        }
        engine.flush();
        // Reverse over [b, e): emits d, c, b (descending), excluding a (< b) and e (== end).
        ListResult res = engine.scanCandies(ScanQuery.reverse(null, CandyKey.of("b"),
                CandyKey.of("e"), null, 100));
        assertThat(res.entries()).extracting(e -> e.key().value()).containsExactly("d", "c", "b");
    }

    // ---- recovery re-opens SSTables ----------------------------------------------------------

    @Test
    void recoverReopensFlushedSSTablesAndKeepsReadingThem() {
        ManualClock clock = new ManualClock(5000);
        BoxEngine ownerA = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, clock, 1L);
        ownerA.putCandy(CandyKey.of("persisted"), bytes("durable"), null, Map.of(), null);
        ownerA.flush(); // now lives in an L0 SSTable referenced by the manifest
        long manifestLedgerId = ownerA.manifestLedgerId();
        ownerA.close();

        engine = BoxEngine.recover(box, CandyboxConfig.defaults(), store, 2, clock, manifestLedgerId, 2L);
        // The recovered owner must re-open the SSTable readers and serve the flushed key.
        assertThat(engine.getCandy(CandyKey.of("persisted"))).isEqualTo(bytes("durable"));
    }

    // ---- GC bookkeeping ----------------------------------------------------------------------

    @Test
    void dropSyrupsIgnoresAnEmptyCollection() {
        engine = newEngine();
        engine.dropSyrups(List.of()); // no-op, must not touch the manifest
        engine.putCandy(CandyKey.of("k"), bytes("v"), null, Map.of(), null);
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v"));
    }
}
