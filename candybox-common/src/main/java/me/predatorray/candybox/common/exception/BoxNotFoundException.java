package me.predatorray.candybox.common.exception;

/** Thrown when an operation targets a Box that does not exist. */
public class BoxNotFoundException extends CandyboxException {

    public BoxNotFoundException(String boxName) {
        super("No such Box: " + boxName);
    }
}
