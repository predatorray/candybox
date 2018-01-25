/*
 * Copyright (c) 2018 the original author or authors.
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

package me.predatorray.candybox;

import me.predatorray.candybox.util.Validations;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ObjectKey {

    public static final int MAXIMUM_KEY_SIZE = 1 << 16;

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private final byte[] binary;

    public ObjectKey(String stringKey) {
        this(stringKey.getBytes(DEFAULT_CHARSET));
    }

    public ObjectKey(byte[] key) {
        Validations.notNull(key);
        Validations.that(key.length, key.length <= MAXIMUM_KEY_SIZE && key.length > 0,
                "The size of a object key must be within the range (0, " + MAXIMUM_KEY_SIZE + "]");
        this.binary = key;
    }

    public String getKeyAsString() {
        return new String(binary, DEFAULT_CHARSET);
    }

    public byte[] getBinary() {
        return Arrays.copyOf(binary, binary.length);
    }

    public int getSize() {
        return binary.length;
    }

    public short getSizeAsUnsignedShort() {
        return (short) binary.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectKey objectKey = (ObjectKey) o;
        return Arrays.equals(binary, objectKey.binary);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(binary);
    }

    @Override
    public String toString() {
        return getKeyAsString();
    }
}
