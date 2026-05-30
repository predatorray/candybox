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

import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;

/**
 * The value stored at the per-Box manifest pointer key in coordination: which manifest ledger is
 * current, and the fencing token of the owner that published it. Advanced only via compare-and-set on
 * the coordination key's version (never a blind set), so a checkpoint and a concurrent edit cannot race.
 *
 * @param ledgerId   the current manifest ledger id
 * @param ownerToken the fencing token of the owner that wrote this pointer
 */
record ManifestPointer(long ledgerId, long ownerToken) {

    private static final byte FORMAT_VERSION = 1;

    byte[] encode() {
        return new BinaryWriter(20)
                .writeByte(FORMAT_VERSION)
                .writeVarLong(ledgerId)
                .writeVarLong(ownerToken)
                .toByteArray();
    }

    static ManifestPointer decode(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int version = r.readByte();
        if (version != FORMAT_VERSION) {
            throw new IllegalStateException("Unsupported ManifestPointer version: " + version);
        }
        return new ManifestPointer(r.readVarLong(), r.readVarLong());
    }
}
