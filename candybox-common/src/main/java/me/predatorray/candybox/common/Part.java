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
package me.predatorray.candybox.common;

import java.util.List;

/**
 * One uniformly-chunked run of a Candy's bytes. A single-PUT Candy has exactly one {@code Part}; a
 * multipart-uploaded Candy is a list of these in part-number order. Each part is internally what a v1
 * {@link CandyLocator} used to be: a uniform-chunked byte run with its own end-to-end CRC.
 *
 * <p>Byte offsets within a Candy are addressed as "byte {@code o} lives in the part with the smallest
 * cumulative {@code partLength} ≥ {@code o}, at chunk index {@code (o - prefix) / chunkSize}". Per-part
 * chunking is uniform (modulo the final chunk in the part); the part boundary is the only place a
 * short-tail chunk can sit in the middle of the object — that is exactly why the data model has to be
 * 1:N rather than a flat segment list (see {@code MULTIPART_RANGE_PLAN.md}).
 *
 * @param partLength total payload bytes in this part (sum of chunk payloads, excluding the 4-byte
 *                   per-chunk CRC header)
 * @param chunkSize  Syrup chunk size used when writing this part; uniform within the part
 * @param crc32c     end-to-end CRC32C of this part's payload bytes
 * @param segments   contiguous Syrup runs holding this part's chunks, in order; never empty for a Part
 */
public record Part(long partLength, int chunkSize, int crc32c, List<SegmentRef> segments) {

    public Part {
        if (partLength < 0) {
            throw new IllegalArgumentException("partLength must be non-negative");
        }
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
        segments = segments == null ? List.of() : List.copyOf(segments);
        if (partLength > 0 && segments.isEmpty()) {
            throw new IllegalArgumentException("a non-empty Part must carry at least one segment");
        }
    }

    /** Total entry count across this part's segments. */
    public long entryCount() {
        long n = 0;
        for (SegmentRef s : segments) {
            n += s.entryCount();
        }
        return n;
    }
}
