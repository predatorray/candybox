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
