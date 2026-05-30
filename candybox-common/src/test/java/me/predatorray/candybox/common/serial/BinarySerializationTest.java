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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import me.predatorray.candybox.common.exception.SerializationException;
import org.junit.jupiter.api.Test;

class BinarySerializationTest {

    @Test
    void roundTripsAllScalarTypes() {
        byte[] bytes = new BinaryWriter()
                .writeByte(200)
                .writeBoolean(true)
                .writeShort(40_000)
                .writeInt(-123456)
                .writeLong(Long.MIN_VALUE)
                .writeVarInt(300)
                .writeVarLong(9_000_000_000L)
                .writeString("héllo")
                .toByteArray();

        BinaryReader r = new BinaryReader(bytes);
        assertThat(r.readByte()).isEqualTo(200);
        assertThat(r.readBoolean()).isTrue();
        assertThat(r.readShort()).isEqualTo(40_000);
        assertThat(r.readInt()).isEqualTo(-123456);
        assertThat(r.readLong()).isEqualTo(Long.MIN_VALUE);
        assertThat(r.readVarInt()).isEqualTo(300);
        assertThat(r.readVarLong()).isEqualTo(9_000_000_000L);
        assertThat(r.readString()).isEqualTo("héllo");
        assertThat(r.hasRemaining()).isFalse();
    }

    @Test
    void readingPastEndThrows() {
        byte[] bytes = new BinaryWriter().writeInt(1).toByteArray();
        BinaryReader r = new BinaryReader(bytes);
        r.readInt();
        assertThatThrownBy(r::readByte).isInstanceOf(SerializationException.class);
    }

    @Test
    void malformedVarintThrows() {
        // Six continuation bytes exceed the legal varint length for an int.
        byte[] bad = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80};
        assertThatThrownBy(() -> new BinaryReader(bad).readVarInt())
                .isInstanceOf(SerializationException.class);
    }
}
