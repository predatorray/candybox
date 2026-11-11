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
package me.predatorray.candybox.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.common.serial.BinaryWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RenameMarkerTest {
    private static final Hlc STAMP = new Hlc(1234, 5, 6);

    @Test
    void preparedReservationCommitAndAcknowledgementRemainDistinctAndRoundTrip() {
        var prepared = new RenameMarker("incarnation", "src", 2, STAMP, "dst", false, false);
        var reserved = prepared.reserve();
        var committed = reserved.commit();
        var acknowledged = committed.acknowledge();
        assertThat(prepared.reserved()).isFalse();
        assertThat(prepared.committed()).isFalse();
        assertThat(reserved.reserved()).isTrue();
        assertThat(reserved.committed()).isFalse();
        assertThat(committed.committed()).isTrue();
        assertThat(committed.completed()).isFalse();
        assertThat(acknowledged.completed()).isTrue();
        assertThat(acknowledged.committed()).isTrue();
        assertThat(acknowledged.reserved()).isTrue();
        for (var marker : new RenameMarker[] {prepared, reserved, committed, acknowledged}) {
            assertThat(RenameMarker.deserialize(marker.serialize())).isEqualTo(marker);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void oldVersionsPreserveTheirDecisionMeaning(int version) {
        for (boolean completed : new boolean[] {false, true}) {
            byte[] encoded = encodeVersion(version, completed);
            var marker = RenameMarker.deserialize(encoded);
            assertThat(marker.incarnation()).isEqualTo("incarnation");
            assertThat(marker.srcKey()).isEqualTo("src");
            assertThat(marker.srcPartition()).isEqualTo(2);
            assertThat(marker.srcHlc()).isEqualTo(STAMP);
            assertThat(marker.dstKey()).isEqualTo("dst");
            assertThat(marker.completed()).isEqualTo(completed);
            // v1 only represented commits; v2 added prepared, v3 added reserved.
            assertThat(marker.committed()).isEqualTo(version == 1);
            assertThat(marker.reserved()).isEqualTo(version != 2);
            assertThat(RenameMarker.deserialize(marker.serialize())).isEqualTo(marker);
        }
    }

    @Test
    void legacyUnversionedMarkerIsAnUnacknowledgedCommit() {
        var marker = RenameMarker.deserialize(encodeLegacy());
        assertThat(marker).isEqualTo(new RenameMarker("", "src", 2, STAMP, "dst", false));
        assertThat(RenameMarker.deserialize(marker.serialize())).isEqualTo(marker);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void rejectsTrailingBytesInEveryVersion(int version) {
        byte[] encoded = encodeVersion(version, false);
        assertThatThrownBy(() -> RenameMarker.deserialize(Arrays.copyOf(encoded, encoded.length + 1)))
                .isInstanceOf(SerializationException.class).hasMessageContaining("Trailing");
    }

    @Test
    void rejectsTrailingLegacyBytesAndUnsupportedVersion() {
        byte[] legacy = encodeLegacy();
        assertThatThrownBy(() -> RenameMarker.deserialize(Arrays.copyOf(legacy, legacy.length + 1)))
                .isInstanceOf(SerializationException.class).hasMessageContaining("Trailing legacy");
        assertThatThrownBy(() -> RenameMarker.deserialize(new byte[] {0, 4}))
                .isInstanceOf(SerializationException.class).hasMessageContaining("Unsupported");
    }

    @Test
    void rejectsTruncatedLegacyAndVersionedRecords() {
        for (byte[] record : new byte[][] {encodeLegacy(), encodeVersion(1, false),
                encodeVersion(2, false), encodeVersion(3, false)}) {
            for (int length = 0; length < record.length; length++) {
                byte[] truncated = Arrays.copyOf(record, length);
                assertThatThrownBy(() -> RenameMarker.deserialize(truncated))
                        .isInstanceOf(SerializationException.class);
            }
        }
    }

    private static byte[] encodeVersion(int version, boolean completed) {
        var writer = new BinaryWriter(128);
        writer.writeByte(0); writer.writeByte(version); writer.writeString("incarnation");
        writeTransfer(writer); writer.writeBoolean(completed);
        if (version >= 2) { writer.writeBoolean(false); }
        if (version >= 3) { writer.writeBoolean(true); }
        return writer.toByteArray();
    }

    private static byte[] encodeLegacy() {
        var writer = new BinaryWriter(128);
        writeTransfer(writer);
        return writer.toByteArray();
    }

    private static void writeTransfer(BinaryWriter writer) {
        writer.writeString("src"); writer.writeVarInt(2);
        writer.writeVarLong(STAMP.physicalMillis()); writer.writeVarInt(STAMP.logicalCounter());
        writer.writeInt(STAMP.nodeId()); writer.writeString("dst");
    }
}
