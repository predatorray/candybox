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

import java.util.Map;
import java.util.TreeMap;
import me.predatorray.candybox.common.Part;

/**
 * In-progress multipart upload state stored in the {@link ManifestState}: per-upload {@code uploadId},
 * the destination {@code key} and prospective object headers, plus the parts already uploaded as a
 * {@code Map<partNumber, Part>}. Lives in the manifest so the state survives owner handover and is
 * fencing-gated on every change.
 *
 * @param uploadId        the upload's identifier (gateway-generated, opaque to the engine)
 * @param key             the destination CandyKey this upload will materialize at on Complete
 * @param contentType     optional content-type to stamp on the final locator (nullable)
 * @param userMetadata    user metadata to stamp on the final locator (never null; empty if none)
 * @param createdAtMillis wall-clock time of the {@code CreateMultipartUpload} call, used by the TTL sweeper
 * @param parts           per-{@code partNumber} part records, sorted ascending by {@code partNumber}
 */
public record MultipartUploadState(String uploadId, String key, String contentType,
                                   Map<String, String> userMetadata, long createdAtMillis,
                                   Map<Integer, Part> parts) {

    public MultipartUploadState {
        if (uploadId == null || uploadId.isEmpty()) {
            throw new IllegalArgumentException("uploadId is required");
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is required");
        }
        userMetadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);
        // Preserve part-number ordering so Complete can iterate naturally and serialization is
        // deterministic across handovers.
        TreeMap<Integer, Part> sorted = new TreeMap<>();
        if (parts != null) {
            sorted.putAll(parts);
        }
        parts = java.util.Collections.unmodifiableMap(sorted);
    }

    /** Returns a copy with {@code partNumber} bound to {@code part} (last-write-wins per partNumber). */
    public MultipartUploadState withPart(int partNumber, Part part) {
        if (partNumber < 1) {
            throw new IllegalArgumentException("partNumber must be >= 1");
        }
        TreeMap<Integer, Part> next = new TreeMap<>(parts);
        next.put(partNumber, part);
        return new MultipartUploadState(uploadId, key, contentType, userMetadata, createdAtMillis,
                next);
    }
}
