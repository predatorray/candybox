package me.predatorray.candybox.common.exception;

/** Thrown when deleting a non-empty Box without {@code force}. */
public class BoxNotEmptyException extends CandyboxException {

    public BoxNotEmptyException(String boxName) {
        super("Box is not empty: " + boxName);
    }
}
