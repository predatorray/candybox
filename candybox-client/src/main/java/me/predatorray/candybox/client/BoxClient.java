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
package me.predatorray.candybox.client;

import java.util.List;
import java.util.Map;

/**
 * Narrow client-side facade over the Box + Candy operations that the admin/dashboard layer
 * consumes. Carved out of {@link CandyboxClient}'s much larger surface so downstream modules can
 * depend on the slice they actually use — and substitute a hand-written fake in unit tests — without
 * depending on the {@link Router}/{@link me.predatorray.candybox.protocol.transport.Transport}
 * wiring beneath. Mirrors the {@code CandyStore} / {@code FakeCandyStore} split in
 * {@code candybox-s3-gateway}.
 *
 * <p>Method signatures are intentionally identical to {@link CandyboxClient}'s so the production
 * class implements this interface without an adapter. Error semantics match the underlying client:
 * methods throw {@link me.predatorray.candybox.common.exception.CandyboxException} (or a subclass)
 * on remote/operational failures.
 */
public interface BoxClient {

    /** Lists the Boxes known to the contacted node. */
    List<String> listBoxes();

    /** True iff the named Box exists with a live owner. */
    boolean headBox(String box);

    /** Creates a new (empty) Box. Fails with a {@code CandyboxException} if it already exists. */
    void createBox(String box);

    /**
     * Deletes a Box. {@code force=false} rejects a non-empty box; {@code force=true} asks the
     * server to tear its Candies down first.
     */
    void deleteBox(String box, boolean force);

    /**
     * Uploads a Candy. {@code contentType} may be {@code null} (server defaults to
     * {@code application/octet-stream}); {@code userMetadata} may be empty; {@code idempotencyToken}
     * may be {@code null} when the caller doesn't need de-dup.
     */
    void putCandy(String box, String key, byte[] data, String contentType,
                  Map<String, String> userMetadata, String idempotencyToken);

    /** Deletes one Candy; missing keys are treated as success by the server (idempotent). */
    void deleteCandy(String box, String key);

    /**
     * Lists Candies in a Box, paged by {@code startAfter}. {@code maxKeys} bounds the page size;
     * the consumer is expected to clamp it before calling.
     */
    CandyboxClient.Listing listCandies(String box, String prefix, String startAfter, int maxKeys);
}
