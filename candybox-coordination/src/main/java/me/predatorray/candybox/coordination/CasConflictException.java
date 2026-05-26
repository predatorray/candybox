package me.predatorray.candybox.coordination;

/**
 * Thrown when a {@code compareAndSet} (or versioned delete) sees a version other than the one
 * expected — a concurrent writer won the race. The caller must re-read and retry; it must never
 * fall back to a blind set, which would clobber a concurrent manifest checkpoint.
 */
public class CasConflictException extends CoordinationException {

    public CasConflictException(String key, long expectedVersion, long actualVersion) {
        super("CAS conflict on key '" + key + "': expected version " + expectedVersion
                + " but found " + actualVersion);
    }
}
