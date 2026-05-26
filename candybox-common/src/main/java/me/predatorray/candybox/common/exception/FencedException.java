package me.predatorray.candybox.common.exception;

/**
 * Thrown when an operation is rejected because the actor's fencing token / lease has been superseded:
 * a zombie owner trying to append to a sealed WAL or manifest, or a zombie compactor trying to commit
 * after its lease expired. The defining check that keeps a stale node from corrupting committed state.
 */
public class FencedException extends CandyboxException {

    public FencedException(String message) {
        super(message);
    }
}
