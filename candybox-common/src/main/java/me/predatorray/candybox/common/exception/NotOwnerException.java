package me.predatorray.candybox.common.exception;

/**
 * Thrown when a node is asked to serve a Box it does not currently own (its ownership lease has not
 * been acquired, or has expired/been superseded). In a cluster the client should re-resolve the
 * Box's owner and retry; Phase 2 routing surfaces this as a {@code MOVED} response.
 */
public class NotOwnerException extends CandyboxException {

    public NotOwnerException(String boxName) {
        super("This node does not own Box: " + boxName);
    }
}
