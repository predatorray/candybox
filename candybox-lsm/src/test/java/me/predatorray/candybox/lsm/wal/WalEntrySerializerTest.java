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
package me.predatorray.candybox.lsm.wal;

import static me.predatorray.candybox.lsm.TestData.hlc;
import static me.predatorray.candybox.lsm.TestData.putMutation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.LimitExceededException;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.common.serial.MutationSerializer;
import me.predatorray.candybox.common.serial.RangeTombstoneSerializer;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import org.junit.jupiter.api.Test;

class WalEntrySerializerTest {

    @Test
    void roundTripsPointMutation() {
        Mutation m = putMutation("k", hlc(10, 1, 2));
        WalEntry decoded = WalEntrySerializer.deserialize(
                WalEntrySerializer.serialize(WalEntry.of(m)));
        assertThat(decoded).isInstanceOf(WalEntry.PointMutation.class);
        assertThat(((WalEntry.PointMutation) decoded).mutation()).isEqualTo(m);
        assertThat(decoded.hlc()).isEqualTo(hlc(10, 1, 2));
    }

    @Test
    void roundTripsRangeDelete() {
        RangeTombstone rt = new RangeTombstone(CandyKey.of("a/"), CandyKey.of("a0"), hlc(20, 0, 1));
        WalEntry decoded = WalEntrySerializer.deserialize(
                WalEntrySerializer.serialize(WalEntry.of(rt)));
        assertThat(decoded).isInstanceOf(WalEntry.RangeDelete.class);
        assertThat(((WalEntry.RangeDelete) decoded).tombstone()).isEqualTo(rt);
        assertThat(decoded.hlc()).isEqualTo(hlc(20, 0, 1));
    }
    @Test
    void batchRoundTripAndReplayObserveMaximumAcrossEveryMemberAndLegacyRecord() {
        Mutation high = putMutation("high", hlc(30, 0, 1));
        Mutation low = putMutation("low", hlc(10, 0, 1));
        WalEntry batch = WalEntry.batch(List.of(high, low));
        assertThat(WalEntrySerializer.deserialize(WalEntrySerializer.serialize(batch))).isEqualTo(batch);
        try (InMemoryLedgerStore store = new InMemoryLedgerStore();
             WriteAheadLog wal = WriteAheadLog.create(store, LedgerConfig.forRole(LedgerRole.WAL))) {
            wal.append(putMutation("legacy", hlc(20, 0, 1)));
            wal.append(batch);
            wal.append(new RangeTombstone(null, null, hlc(25, 0, 1)));
            try (ReadableLedger recovered = store.recoverOpen(wal.ledgerId())) {
                WriteAheadLog.ReplayResult replay = WriteAheadLog.replay(recovered);
                assertThat(replay.entries()).hasSize(3);
                assertThat(replay.maxHlc()).isEqualTo(high.hlc());
            }
        }
    }

    @Test
    void decodesLegacyKindFramingWithoutChangingItsBytes() {
        Mutation mutation = putMutation("old", hlc(10, 0, 1));
        byte[] point = new BinaryWriter(64).writeByte(1)
                .writeRaw(MutationSerializer.serialize(mutation)).toByteArray();
        assertThat(WalEntrySerializer.deserialize(point)).isEqualTo(WalEntry.of(mutation));
        assertThat(WalEntrySerializer.serialize(WalEntry.of(mutation))).isEqualTo(point);
        RangeTombstone range = new RangeTombstone(null, null, hlc(20, 0, 1));
        byte[] deletion = new BinaryWriter(64).writeByte(2)
                .writeRaw(RangeTombstoneSerializer.serialize(range)).toByteArray();
        assertThat(WalEntrySerializer.deserialize(deletion)).isEqualTo(WalEntry.of(range));
        assertThat(WalEntrySerializer.serialize(WalEntry.of(range))).isEqualTo(deletion);
    }

    @Test
    void rejectsEveryTruncationUnknownVersionsAndMalformedBatchFraming() {
        byte[] batch = WalEntrySerializer.serialize(WalEntry.batch(List.of(
                putMutation("a", hlc(1, 0, 1)), putMutation("b", hlc(2, 0, 1)))));
        for (int length = 0; length < batch.length; length++) {
            byte[] truncated = Arrays.copyOf(batch, length);
            assertThatThrownBy(() -> WalEntrySerializer.deserialize(truncated))
                    .isInstanceOf(SerializationException.class);
        }
        for (byte[] corrupt : List.of(new byte[]{99}, new byte[]{3, 99},
                new byte[]{3, 1, 0}, new byte[]{3, 1, (byte) 129, 1},
                new byte[]{3, 1, (byte) 255, (byte) 255, (byte) 255, (byte) 255, 7},
                Arrays.copyOf(batch, batch.length + 1))) {
            assertThatThrownBy(() -> WalEntrySerializer.deserialize(corrupt))
                    .isInstanceOf(SerializationException.class);
        }
        byte[] badMember = new BinaryWriter(32).writeByte(3).writeByte(1).writeVarInt(1)
                .writeBytes(new byte[]{99}).toByteArray();
        assertThatThrownBy(() -> WalEntrySerializer.deserialize(badMember))
                .isInstanceOf(SerializationException.class);
        byte[] validMember = MutationSerializer.serialize(putMutation("a", hlc(1, 0, 1)));
        byte[] memberWithJunk = Arrays.copyOf(validMember, validMember.length + 1);
        byte[] trailingMember = new BinaryWriter(256).writeByte(3).writeByte(1).writeVarInt(1)
                .writeBytes(memberWithJunk).toByteArray();
        assertThatThrownBy(() -> WalEntrySerializer.deserialize(trailingMember))
                .isInstanceOf(SerializationException.class);
    }

    @Test
    void boundsBatchCountAndActualEncodedEntrySize() {
        Mutation mutation = putMutation("x".repeat(16_384), hlc(1, 0, 1));
        assertThatThrownBy(() -> WalEntry.batch(List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WalEntry.batch(Collections.nCopies(129, mutation)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WalEntrySerializer.serialize(
                WalEntry.batch(Collections.nCopies(128, mutation))))
                .isInstanceOf(LimitExceededException.class);
        byte[] oversized = new byte[WalEntrySerializer.MAX_BATCH_BYTES + 1];
        oversized[0] = 3;
        assertThatThrownBy(() -> WalEntrySerializer.deserialize(oversized))
                .isInstanceOf(SerializationException.class);
    }

}
