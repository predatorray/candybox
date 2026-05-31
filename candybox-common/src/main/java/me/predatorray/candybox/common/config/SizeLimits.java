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
package me.predatorray.candybox.common.config;

/**
 * The configurable size limits from DESIGN.md §6. Validated at both the client (fail fast) and the
 * node (authoritative).
 *
 * @param chunkSizeBytes        Syrup entry payload size; must be below the BK max frame size minus
 *                              overhead (validated against the BK client config on startup). Default 1 MiB.
 * @param maxCandyKeyBytes      max CandyKey length in UTF-8 bytes. Default 1 KiB.
 * @param maxUserMetadataBytes  max total user-metadata size. Default 8 KiB.
 * @param maxLocatorBytes       hard cap on a serialized CandyLocator. Default 256 KiB (large enough
 *                              for a 10,000-part multipart Candy; see {@code MULTIPART_RANGE_PLAN.md}).
 * @param maxCandySizeBytes     max Candy size; {@code 0} means effectively unbounded. Default 0.
 */
public record SizeLimits(
        int chunkSizeBytes,
        int maxCandyKeyBytes,
        int maxUserMetadataBytes,
        int maxLocatorBytes,
        long maxCandySizeBytes) {

    public static final int DEFAULT_CHUNK_SIZE = 1 << 20;        // 1 MiB
    public static final int DEFAULT_MAX_KEY_BYTES = 1 << 10;     // 1 KiB
    public static final int DEFAULT_MAX_METADATA_BYTES = 8 << 10; // 8 KiB
    public static final int DEFAULT_MAX_LOCATOR_BYTES = 256 << 10; // 256 KiB
    public static final long DEFAULT_MAX_CANDY_SIZE = 0L;        // unbounded

    public SizeLimits {
        if (chunkSizeBytes < 1) {
            throw new IllegalArgumentException("chunkSizeBytes must be positive");
        }
        if (maxCandyKeyBytes < 1) {
            throw new IllegalArgumentException("maxCandyKeyBytes must be positive");
        }
        if (maxUserMetadataBytes < 0 || maxLocatorBytes < 1) {
            throw new IllegalArgumentException("invalid metadata/locator limits");
        }
        if (maxCandySizeBytes < 0) {
            throw new IllegalArgumentException("maxCandySizeBytes must be >= 0 (0 = unbounded)");
        }
    }

    public static SizeLimits defaults() {
        return new SizeLimits(
                DEFAULT_CHUNK_SIZE,
                DEFAULT_MAX_KEY_BYTES,
                DEFAULT_MAX_METADATA_BYTES,
                DEFAULT_MAX_LOCATOR_BYTES,
                DEFAULT_MAX_CANDY_SIZE);
    }

    /** Whether a Candy of {@code length} bytes is within the configured maximum. */
    public boolean isCandySizeAllowed(long length) {
        return maxCandySizeBytes == 0 || length <= maxCandySizeBytes;
    }
}
