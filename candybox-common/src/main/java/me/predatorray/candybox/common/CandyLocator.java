package me.predatorray.candybox.common;

import java.util.List;
import java.util.Map;

/**
 * The compact LSM value: a pointer to where a Candy's bytes live (in Syrups), plus the metadata the
 * read path needs without touching the data. The LSM tree stores only these, never Candy bytes.
 *
 * <p>The {@link #hlc()} is the LWW key. A {@link LocatorType#DELETE} tombstone carries no segments and
 * zero content length; it shadows any earlier PUT for the key until it is GC'd under the
 * bottommost-level + time-bound rule.
 *
 * @param hlc            the HLC timestamp (LWW key)
 * @param type           PUT or DELETE
 * @param contentLength  Candy length in bytes (0 for tombstone)
 * @param chunkSize      Syrup chunk size used when writing (0 for tombstone)
 * @param contentType    optional MIME-ish content type (nullable)
 * @param userMetadata   small user metadata map (never null; empty if none)
 * @param crc32c         whole-object end-to-end CRC32C (0 for tombstone)
 * @param createdAtMillis wall-clock creation time, informational only
 * @param segments       Syrup segments holding the bytes, in order (empty for tombstone)
 */
public record CandyLocator(
        Hlc hlc,
        LocatorType type,
        long contentLength,
        int chunkSize,
        String contentType,
        Map<String, String> userMetadata,
        int crc32c,
        long createdAtMillis,
        List<SegmentRef> segments) {

    public CandyLocator {
        if (hlc == null || type == null) {
            throw new IllegalArgumentException("hlc and type are required");
        }
        userMetadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);
        segments = segments == null ? List.of() : List.copyOf(segments);
        if (type == LocatorType.DELETE) {
            if (!segments.isEmpty() || contentLength != 0) {
                throw new IllegalArgumentException("DELETE tombstone must carry no segments/content");
            }
        } else if (contentLength < 0) {
            throw new IllegalArgumentException("contentLength must be non-negative");
        }
    }

    public boolean isTombstone() {
        return type == LocatorType.DELETE;
    }

    /** Builds a tombstone locator for a deletion at the given HLC. */
    public static CandyLocator tombstone(Hlc hlc, long createdAtMillis) {
        return new CandyLocator(hlc, LocatorType.DELETE, 0, 0, null, Map.of(), 0, createdAtMillis,
                List.of());
    }
}
