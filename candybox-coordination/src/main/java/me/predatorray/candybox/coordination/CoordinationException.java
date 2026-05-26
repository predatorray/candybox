package me.predatorray.candybox.coordination;

import me.predatorray.candybox.common.exception.CandyboxException;

/** Base of coordination-layer failures (ZooKeeper or its in-memory fake). */
public class CoordinationException extends CandyboxException {

    public CoordinationException(String message) {
        super(message);
    }

    public CoordinationException(String message, Throwable cause) {
        super(message, cause);
    }
}
