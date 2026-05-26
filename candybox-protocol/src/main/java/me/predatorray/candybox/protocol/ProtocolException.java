package me.predatorray.candybox.protocol;

import me.predatorray.candybox.common.exception.CandyboxException;

/** Thrown for malformed frames: bad magic/version, an oversized length prefix, or truncation. */
public class ProtocolException extends CandyboxException {

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
