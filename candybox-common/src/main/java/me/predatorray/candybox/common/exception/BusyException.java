package me.predatorray.candybox.common.exception;

/**
 * Thrown (and surfaced on the wire as a {@code BUSY} response) when a node is under write-stall
 * backpressure — e.g. too many L0 SSTables awaiting compaction. The operation is retriable; the
 * client should back off rather than treat it as a hard failure.
 */
public class BusyException extends CandyboxException {

    public BusyException(String message) {
        super(message);
    }
}
