package me.predatorray.candybox.coordination;

/** Thrown when an operation is attempted with a lease that is no longer valid (expired/superseded). */
public class LeaseExpiredException extends CoordinationException {

    public LeaseExpiredException(String resource, long fencingToken) {
        super("Lease on '" + resource + "' (token " + fencingToken + ") is no longer valid");
    }
}
