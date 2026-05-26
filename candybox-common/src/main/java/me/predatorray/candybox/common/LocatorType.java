package me.predatorray.candybox.common;

import me.predatorray.candybox.common.exception.SerializationException;

/** Whether a {@link CandyLocator} records a live value (PUT) or a deletion marker (DELETE tombstone). */
public enum LocatorType {
    PUT((byte) 1),
    DELETE((byte) 2);

    private final byte code;

    LocatorType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public boolean isTombstone() {
        return this == DELETE;
    }

    public static LocatorType fromCode(int code) {
        return switch (code) {
            case 1 -> PUT;
            case 2 -> DELETE;
            default -> throw new SerializationException("Unknown LocatorType code: " + code);
        };
    }
}
