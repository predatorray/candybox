package me.predatorray.candybox.common;

/**
 * A range deletion marker: it shadows every key in the half-open interval
 * {@code [startInclusive, endExclusive)} whose own locator HLC is older than {@link #hlc()}. Unlike a
 * point {@link LocatorType#DELETE} tombstone (which targets one key), a range tombstone deletes a whole
 * key span with a single O(1) record — the LSM primitive behind {@code deleteRange}.
 *
 * <p>Both bounds are nullable: a null {@code startInclusive} means "from the start of the keyspace" and
 * a null {@code endExclusive} means "to the end". It carries no Syrup segments, so it never pins data;
 * the bytes it shadows are reclaimed when compaction drops the covered point locators (see DESIGN §9).
 *
 * @param startInclusive inclusive lower bound (nullable = unbounded below)
 * @param endExclusive   exclusive upper bound (nullable = unbounded above)
 * @param hlc            the deletion timestamp (LWW key); only strictly-older keys are shadowed
 */
public record RangeTombstone(CandyKey startInclusive, CandyKey endExclusive, Hlc hlc) {

    public RangeTombstone {
        if (hlc == null) {
            throw new IllegalArgumentException("hlc is required");
        }
        if (startInclusive != null && endExclusive != null
                && startInclusive.compareTo(endExclusive) >= 0) {
            throw new IllegalArgumentException(
                    "range tombstone start must be strictly less than end: "
                            + startInclusive.value() + " >= " + endExclusive.value());
        }
    }

    /** Whether {@code key} falls within {@code [startInclusive, endExclusive)}. */
    public boolean covers(CandyKey key) {
        if (startInclusive != null && key.compareTo(startInclusive) < 0) {
            return false;
        }
        return endExclusive == null || key.compareTo(endExclusive) < 0;
    }
}
