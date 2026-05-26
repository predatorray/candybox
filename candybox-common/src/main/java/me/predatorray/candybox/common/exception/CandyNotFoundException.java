package me.predatorray.candybox.common.exception;

/** Thrown when reading a Candy key that has no live value (never written, or tombstoned). */
public class CandyNotFoundException extends CandyboxException {

    public CandyNotFoundException(String boxName, String candyKey) {
        super("No such Candy: box=" + boxName + " key=" + candyKey);
    }
}
