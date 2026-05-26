package me.predatorray.candybox.coordination;

/**
 * A time-bounded ownership grant over a named resource (a Box's ownership, a compaction task). The
 * holder is the leader/owner only while {@link #isValid()} and must {@link #renew()} before the TTL
 * elapses.
 *
 * <p>{@link #fencingToken()} is the crux of Candybox's safety: it is monotonically increasing across
 * successive acquisitions of the same resource, so a later owner always has a strictly higher token.
 * Every state-mutating manifest append and every compaction/GC commit carries this token and is
 * rejected if a higher token has since been issued — fencing out a zombie owner whose lease lapsed
 * but who has not yet noticed.
 */
public interface Lease {

    /** The resource this lease grants ownership of. */
    String resource();

    /** The node that holds the lease. */
    int ownerNodeId();

    /** The monotonically increasing fencing token for this acquisition. */
    long fencingToken();

    /** Whether the lease is currently held (not expired, released, or superseded by a newer holder). */
    boolean isValid();

    /**
     * Extends the lease by its TTL from now.
     *
     * @throws LeaseExpiredException if the lease has already been lost and cannot be renewed
     */
    void renew();

    /** Voluntarily relinquishes the lease. Idempotent. */
    void release();
}
