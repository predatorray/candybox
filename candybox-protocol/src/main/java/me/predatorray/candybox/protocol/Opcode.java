package me.predatorray.candybox.protocol;

/**
 * The wire opcodes. Requests in the 1–19 range, responses in 20+. {@code RESPONSE_BUSY} is the
 * dedicated retriable backpressure signal returned under write-stall.
 */
public enum Opcode {
    CREATE_BOX(1),
    DELETE_BOX(2),
    LIST_BOXES(3),
    HEAD_BOX(4),

    PUT_CANDY(10),
    GET_CANDY(11),
    HEAD_CANDY(12),
    DELETE_CANDY(13),
    LIST_CANDIES(14),
    COPY_CANDY(15),
    RENAME_CANDY(16),

    RESPONSE_OK(20),
    RESPONSE_ERROR(21),
    RESPONSE_BUSY(22),
    RESPONSE_NOT_FOUND(23),
    RESPONSE_CANDY_DATA(24),
    RESPONSE_LIST(25),
    RESPONSE_HEAD(26),
    /** The target Box is owned by another node; the client should re-route (see routing, WS5). */
    RESPONSE_MOVED(27),
    RESPONSE_BOX_LIST(28);

    private final int code;

    Opcode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static Opcode fromCode(int code) {
        for (Opcode op : values()) {
            if (op.code == code) {
                return op;
            }
        }
        throw new ProtocolException("Unknown opcode: " + code);
    }
}
