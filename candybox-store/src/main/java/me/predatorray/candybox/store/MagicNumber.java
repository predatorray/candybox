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

package me.predatorray.candybox.store;

import me.predatorray.candybox.util.Validations;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class MagicNumber {

    private final int value;

    public MagicNumber(String textInterpretation) {
        this(ByteBuffer.wrap(Validations.that(
                Validations.notNull(textInterpretation).getBytes(StandardCharsets.ISO_8859_1),
                bytes -> bytes.length == 4,
                "The text interpretation must be a 4-bytes string encoding in ISO_8859_1")).getInt());
    }

    public MagicNumber(int value) {
        this.value = value;
    }

    public byte[] toBytes() {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public int toInteger() {
        return value;
    }

    public String getTextInterpretation() {
        return new String(toBytes(), StandardCharsets.ISO_8859_1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MagicNumber that = (MagicNumber) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return getTextInterpretation();
    }
}
