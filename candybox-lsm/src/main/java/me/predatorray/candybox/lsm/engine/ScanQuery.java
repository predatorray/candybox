package me.predatorray.candybox.lsm.engine;

import me.predatorray.candybox.common.CandyKey;

/**
 * A parameterized listing over the sorted keyspace: an optional {@code [startInclusive, endExclusive)}
 * window, optionally narrowed to a {@code prefix}, walked in either {@link ScanDirection}, with a
 * continuation {@code cursorExclusive} and a page size.
 *
 * <p>All key bounds are nullable: a null lower bound means "from the start of the keyspace", a null
 * upper bound means "to the end". {@code prefix} is a convenience that intersects the window with the
 * prefix range; {@code cursorExclusive} is the key returned by a previous page (exclusive, interpreted
 * in the scan direction). The classic {@code listCandies(prefix, startAfter, maxKeys)} is exactly
 * {@link #forward(String, CandyKey, int)}.
 *
 * @param prefix          optional key prefix (nullable)
 * @param startInclusive  inclusive lower key bound (nullable = unbounded below)
 * @param endExclusive    exclusive upper key bound (nullable = unbounded above)
 * @param cursorExclusive continuation cursor, exclusive in the scan direction (nullable)
 * @param direction       scan order
 * @param maxKeys         page size; values {@code <= 0} fall back to {@link #DEFAULT_MAX_KEYS}
 */
public record ScanQuery(
        String prefix,
        CandyKey startInclusive,
        CandyKey endExclusive,
        CandyKey cursorExclusive,
        ScanDirection direction,
        int maxKeys) {

    public static final int DEFAULT_MAX_KEYS = 1000;

    public ScanQuery {
        if (direction == null) {
            throw new IllegalArgumentException("direction is required");
        }
    }

    /** The classic forward listing: prefix + exclusive {@code startAfter} cursor. */
    public static ScanQuery forward(String prefix, CandyKey startAfter, int maxKeys) {
        return new ScanQuery(prefix, null, null, startAfter, ScanDirection.FORWARD, maxKeys);
    }

    /** A bounded forward scan over {@code [start, end)}. */
    public static ScanQuery forwardRange(CandyKey startInclusive, CandyKey endExclusive, int maxKeys) {
        return new ScanQuery(null, startInclusive, endExclusive, null, ScanDirection.FORWARD, maxKeys);
    }

    /** A reverse scan, optionally bounded by {@code [start, end)} and narrowed by {@code prefix}. */
    public static ScanQuery reverse(String prefix, CandyKey startInclusive, CandyKey endExclusive,
                                    CandyKey cursorExclusive, int maxKeys) {
        return new ScanQuery(prefix, startInclusive, endExclusive, cursorExclusive,
                ScanDirection.REVERSE, maxKeys);
    }

    public int effectiveMaxKeys() {
        return maxKeys > 0 ? maxKeys : DEFAULT_MAX_KEYS;
    }
}
