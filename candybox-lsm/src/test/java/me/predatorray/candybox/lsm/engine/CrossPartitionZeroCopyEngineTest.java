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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.auth.ObjectAcl;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.lsm.manifest.RenameIntent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Engine-level coverage of the cross-partition zero-copy primitives: {@link BoxEngine#resolveLocator},
 * {@link BoxEngine#zeroCopyPut}, the LWW {@link BoxEngine#deleteCandyConditional}, the rename-intent
 * journal, and the published referenced-Syrup set the Box-global GC relies on.
 *
 * <p>Two engines over one shared {@link InMemoryLedgerStore} stand in for two partitions of a Box:
 * the destination engine reuses the source engine's Syrup segments verbatim, and reads stream those
 * segments straight from the shared store — no bytes are copied.
 */
class CrossPartitionZeroCopyEngineTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final BoxName box = BoxName.of("my-box");
    private BoxEngine src;
    private BoxEngine dst;

    @AfterEach
    void tearDown() {
        if (src != null) {
            src.close();
        }
        if (dst != null) {
            dst.close();
        }
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void zeroCopyPutReusesSourceSegmentsAcrossPartitions() {
        src = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        dst = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 2, new ManualClock(1000), 1L);
        src.putCandy(CandyKey.of("src"), bytes("payload"), "text/plain", Map.of("k", "v"), null);
        src.flush();

        CandyLocator locator = src.resolveLocator(CandyKey.of("src"));
        long srcSyrups = dst.manifestState().referencedSyrups().size();

        dst.zeroCopyPut(CandyKey.of("dst"), locator.parts(), locator.contentType(),
                locator.userMetadata(), locator.createdAtMillis(), locator.acl(), null);

        // Destination resolves the source's bytes without the source engine writing anything new,
        // and the destination wrote no new Syrup of its own (it points at the source's segment).
        assertThat(dst.getCandy(CandyKey.of("dst"))).isEqualTo(bytes("payload"));
        assertThat(dst.headCandy(CandyKey.of("dst")).contentType()).isEqualTo("text/plain");
        dst.flush();
        assertThat(dst.manifestState().referencedSyrups()).hasSize((int) srcSyrups + 1);
        // The destination references the source's Syrup id (the shared segment).
        long sharedSyrup = locator.parts().get(0).segments().get(0).syrupId();
        assertThat(dst.referencedSyrups()).contains(sharedSyrup);
    }

    @Test
    void conditionalDeleteHonorsLwwGuard() {
        src = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        src.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);
        Hlc v1 = src.resolveLocator(CandyKey.of("k")).hlc();

        // A stale guard (a different HLC) is a no-op; the key stays live.
        assertThat(src.deleteCandyConditional(CandyKey.of("k"), Hlc.MIN)).isFalse();
        assertThat(src.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v1"));

        // The matching guard tombstones the key.
        assertThat(src.deleteCandyConditional(CandyKey.of("k"), v1)).isTrue();
        assertThatThrownBy(() -> src.getCandy(CandyKey.of("k")))
                .isInstanceOf(CandyNotFoundException.class);
    }

    @Test
    void conditionalDeleteNeverClobbersARecreatedSource() {
        src = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        Hlc oldHlc = src.putCandy(CandyKey.of("k"), bytes("old"), null, Map.of(), null).hlc();
        // The key is legitimately re-put after the rename began (a strictly newer HLC).
        src.putCandy(CandyKey.of("k"), bytes("new"), null, Map.of(), null);

        // A finalize keyed on the stale source HLC must not delete the fresh value.
        assertThat(src.deleteCandyConditional(CandyKey.of("k"), oldHlc)).isFalse();
        assertThat(src.getCandy(CandyKey.of("k"))).isEqualTo(bytes("new"));
    }

    @Test
    void renameIntentsAreRecordedListedClearedAndSurviveHandover() {
        src = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        RenameIntent intent = new RenameIntent("tok-1", "src", new Hlc(1000, 0, 1), "dst", 3, 1000);
        src.recordRenameIntent(intent);
        assertThat(src.listRenameIntents()).containsExactly(intent);

        long manifestLedgerId = src.manifestLedgerId();
        src.close();
        src = null;

        // A new owner takes over and replays the intent from the manifest.
        dst = BoxEngine.recover(box, CandyboxConfig.defaults(), store, 2, new ManualClock(1000),
                manifestLedgerId, 2L);
        assertThat(dst.listRenameIntents()).containsExactly(intent);

        dst.clearRenameIntent("tok-1");
        assertThat(dst.listRenameIntents()).isEmpty();
    }

    @Test
    void zeroCopyPutIsIdempotentUnderRetryToken() {
        src = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
        dst = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 2, new ManualClock(1000), 1L);
        src.putCandy(CandyKey.of("src"), bytes("payload"), null, Map.of(), null);
        List<me.predatorray.candybox.common.Part> parts = src.resolveLocator(CandyKey.of("src")).parts();

        CandyMetadata first = dst.zeroCopyPut(CandyKey.of("dst"), parts, null, Map.of(), 0L,
                ObjectAcl.NONE, "tok");
        CandyMetadata retry = dst.zeroCopyPut(CandyKey.of("dst"), parts, null, Map.of(), 0L,
                ObjectAcl.NONE, "tok");
        assertThat(retry.hlc()).isEqualTo(first.hlc());
    }
}
