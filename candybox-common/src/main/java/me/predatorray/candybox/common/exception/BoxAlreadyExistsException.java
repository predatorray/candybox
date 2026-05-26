package me.predatorray.candybox.common.exception;

/** Thrown when creating a Box whose name is already taken. */
public class BoxAlreadyExistsException extends CandyboxException {

    public BoxAlreadyExistsException(String boxName) {
        super("Box already exists: " + boxName);
    }
}
