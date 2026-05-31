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
import java.util.Map;

/**
 * The compact LSM value: a pointer to where a Candy's bytes live (in Syrups), plus the metadata the
 * read path needs without touching the data. The LSM tree stores only these, never Candy bytes.
 *
 * <p>The {@link #hlc()} is the LWW key. A {@link LocatorType#DELETE} tombstone carries no parts and
 * shadows any earlier PUT for the key until it is GC'd under the bottommost-level + time-bound rule.
 *
 * <p><b>v2 shape (post multipart):</b> a Candy's bytes are described as an ordered {@code List<Part>}.
 * A single-PUT or {@code copy/rename} produces a one-element list; a multipart-uploaded Candy stitches
 * its parts together in part-number order. Each {@link Part} is internally uniform-chunked with its
 * own end-to-end CRC32C — short-tail chunks therefore only sit at part boundaries, where the read path
 * knows to look (see {@code MULTIPART_RANGE_PLAN.md}). The v1-shaped accessors
 * ({@link #contentLength()}, {@link #chunkSize()}, {@link #crc32c()}, {@link #segments()}) are kept
 * for callers that don't care about parts — derived from the part list.
 *
 * @param hlc             the HLC timestamp (LWW key)
 * @param type            PUT or DELETE
 * @param contentType     optional MIME-ish content type (nullable)
 * @param userMetadata    small user metadata map (never null; empty if none)
 * @param createdAtMillis wall-clock creation time, informational only
 * @param parts           the Candy's bytes as an ordered list of parts; empty for tombstone
 */
public record CandyLocator(
        Hlc hlc,
        LocatorType type,
        String contentType,
        Map<String, String> userMetadata,
        long createdAtMillis,
        List<Part> parts) {

    public CandyLocator {
        if (hlc == null || type == null) {
            throw new IllegalArgumentException("hlc and type are required");
        }
        userMetadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);
        parts = parts == null ? List.of() : List.copyOf(parts);
        if (type == LocatorType.DELETE && !parts.isEmpty()) {
            throw new IllegalArgumentException("DELETE tombstone must carry no parts");
        }
    }

    public boolean isTombstone() {
        return type == LocatorType.DELETE;
    }

    /**
     * Total Candy length in bytes, summed across parts. {@code 0} for tombstones.
     */
    public long contentLength() {
        long total = 0;
        for (Part p : parts) {
            total += p.partLength();
        }
        return total;
    }

    /**
     * The first part's chunk size, or {@code 0} for tombstones. Useful for v1-style single-part
     * callers (heuristics like memtable-size accounting); multipart callers should walk {@link #parts()}
     * for per-part chunk sizes.
     */
    public int chunkSize() {
        return parts.isEmpty() ? 0 : parts.get(0).chunkSize();
    }

    /**
     * Convenience CRC accessor preserved for v1-style callers:
     * <ul>
     *   <li>tombstone → {@code 0}</li>
     *   <li>single-part → that part's end-to-end CRC32C (identical to the v1 whole-object CRC)</li>
     *   <li>multipart → first part's CRC; multipart-aware callers should walk {@link #parts()} and
     *   compose a multipart-style ETag from per-part CRCs</li>
     * </ul>
     */
    public int crc32c() {
        return parts.isEmpty() ? 0 : parts.get(0).crc32c();
    }

    /**
     * Flattened Syrup segments across every part, in object order. Equivalent to the v1
     * {@code segments} field for single-part locators; multipart locators preserve part boundaries
     * via {@link #parts()} but this accessor exists for callers that only need the Syrup-reference
     * set (GC, SSTable flush bookkeeping, etc.).
     */
    public List<SegmentRef> segments() {
        if (parts.isEmpty()) {
            return List.of();
        }
        if (parts.size() == 1) {
            return parts.get(0).segments();
        }
        java.util.ArrayList<SegmentRef> all = new java.util.ArrayList<>();
        for (Part p : parts) {
            all.addAll(p.segments());
        }
        return java.util.Collections.unmodifiableList(all);
    }

    /** Builds a tombstone locator for a deletion at the given HLC. */
    public static CandyLocator tombstone(Hlc hlc, long createdAtMillis) {
        return new CandyLocator(hlc, LocatorType.DELETE, null, Map.of(), createdAtMillis, List.of());
    }

    /** Builds a single-part PUT locator (the common case for normal PUT / copy / rename). */
    public static CandyLocator singlePart(Hlc hlc, long contentLength, int chunkSize, String contentType,
                                          Map<String, String> userMetadata, int crc32c,
                                          long createdAtMillis, List<SegmentRef> segments) {
        Part part = new Part(contentLength, chunkSize, crc32c, segments);
        return new CandyLocator(hlc, LocatorType.PUT, contentType, userMetadata, createdAtMillis,
                List.of(part));
    }
}
