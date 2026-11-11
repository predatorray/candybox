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
package me.predatorray.candybox.common.serial;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.bloom.BloomFilter;
import me.predatorray.candybox.common.exception.SerializationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CorruptInputDecodingTest {
    @Test
    void unterminatedVarlongIsRejected() {
        byte[] bytes = new byte[11];
        Arrays.fill(bytes, (byte) 0x80);
        assertThatThrownBy(() -> new BinaryReader(bytes).readVarLong())
                .isInstanceOf(SerializationException.class)
                .hasMessage("Malformed varlong (too long)");
    }

    @ParameterizedTest
    @ValueSource(ints = {1024, 65536})
    void invalidByteArrayLengthIsRejected(int length) {
        byte[] bytes = new BinaryWriter().writeVarInt(length).toByteArray();
        assertThatThrownBy(() -> new BinaryReader(bytes).readBytes())
                .isInstanceOf(SerializationException.class).hasMessageContaining("Buffer underflow");
    }

    @Test
    void negativeByteArrayLengthIsRejected() {
        // The writer rejects negative lengths; encode the corrupt unsigned LEB128 value directly.
        byte[] bytes = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0f};
        assertThatThrownBy(() -> new BinaryReader(bytes).readBytes())
                .isInstanceOf(SerializationException.class).hasMessageContaining("Buffer underflow");
    }

    @Test
    void unknownBloomFilterVersionIsRejected() {
        byte[] bytes = filterBytes();
        bytes[0] = 127;
        assertThatThrownBy(() -> BloomFilter.deserialize(bytes))
                .isInstanceOf(SerializationException.class)
                .hasMessage("Unsupported BloomFilter format version: 127");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 1})
    void bloomFilterBitArrayMustMatchDeclaredBitCount(int sizeChange) {
        BinaryReader reader = new BinaryReader(filterBytes());
        int version = reader.readByte();
        int numBits = reader.readVarInt();
        int numHashes = reader.readVarInt();
        byte[] bits = reader.readBytes();
        byte[] malformed = new BinaryWriter().writeByte(version).writeVarInt(numBits)
                .writeVarInt(numHashes).writeBytes(Arrays.copyOf(bits, bits.length + sizeChange))
                .toByteArray();
        assertThatThrownBy(() -> BloomFilter.deserialize(malformed))
                .isInstanceOf(SerializationException.class)
                .hasMessage("BloomFilter bit array size mismatch");
    }

    @Test
    void unknownLocatorTypeIsRejected() {
        assertThatThrownBy(() -> LocatorType.fromCode(127))
                .isInstanceOf(SerializationException.class).hasMessage("Unknown LocatorType code: 127");
    }

    private static byte[] filterBytes() {
        return BloomFilter.build(List.of(new byte[] {1, 2, 3}), 10).serialize();
    }
}
