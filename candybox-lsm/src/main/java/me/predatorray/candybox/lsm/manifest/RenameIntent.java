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

import me.predatorray.candybox.common.Hlc;

/**
 * A durable record, held in the <em>source</em> partition's manifest, of a cross-partition
 * {@code rename} that is in flight: the destination's zero-copy {@code put} has been (or is being)
 * written in another partition, and the source partition still owes the conditional delete of
 * {@code srcKey}.
 *
 * <p>The intent is the cross-key analog of an in-flight {@link MultipartUploadState} — it is
 * serialized into {@link ManifestEdit} and replayed on handover, so a new source owner re-acquires
 * the obligation. The source owner finalizes it (on its maintenance loop, or synchronously via
 * {@code CompleteRename}, or after handover replay) by checking the coordination rendezvous marker
 * {@code boxes/<box>/renames/<token>}: present ⇒ tombstone {@code srcKey} (LWW-conditioned on
 * {@code srcHlc}, so a legitimately re-{@code put} source is never clobbered) and clear the intent;
 * absent past the abandon window ⇒ drop the intent (the rename never reached the destination).
 *
 * @param token         the rename's idempotency/rendezvous token (also the coordination marker key)
 * @param srcKey        the source key to delete once the destination is confirmed durable
 * @param srcHlc        the HLC of the source locator captured at prepare time (LWW delete guard)
 * @param dstKey        the destination key (informational; the marker confirms the destination put)
 * @param dstPartition  the destination key's partition (informational / diagnostics)
 * @param createdAtMillis when the intent was recorded, used to abandon stale intents
 */
public record RenameIntent(String token, String srcKey, Hlc srcHlc, String dstKey, int dstPartition,
                           long createdAtMillis) {

    public RenameIntent {
        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("token is required");
        }
        if (srcKey == null || srcKey.isEmpty()) {
            throw new IllegalArgumentException("srcKey is required");
        }
        if (dstKey == null || dstKey.isEmpty()) {
            throw new IllegalArgumentException("dstKey is required");
        }
        if (srcHlc == null) {
            throw new IllegalArgumentException("srcHlc is required");
        }
    }
}
