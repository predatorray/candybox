/*
 * Copyright (c) 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    DELETE_RANGE(17),
    RANGE_GET_CANDY(18),
    CREATE_MULTIPART_UPLOAD(19),
    UPLOAD_PART(30),
    COMPLETE_MULTIPART_UPLOAD(31),
    ABORT_MULTIPART_UPLOAD(32),
    LIST_MULTIPART_UPLOADS(33),
    LIST_PARTS(34),
    UPLOAD_PART_COPY(35),
    BOX_INFO(36),

    RESPONSE_OK(20),
    RESPONSE_ERROR(21),
    RESPONSE_BUSY(22),
    RESPONSE_NOT_FOUND(23),
    RESPONSE_CANDY_DATA(24),
    RESPONSE_LIST(25),
    RESPONSE_HEAD(26),
    /** The target Box is owned by another node; the client should re-route (see routing, WS5). */
    RESPONSE_MOVED(27),
    RESPONSE_BOX_LIST(28),
    RESPONSE_CREATE_MULTIPART(40),
    RESPONSE_UPLOAD_PART(41),
    RESPONSE_LIST_MULTIPART_UPLOADS(42),
    RESPONSE_LIST_PARTS(43),
    RESPONSE_BOX_INFO(44);

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
