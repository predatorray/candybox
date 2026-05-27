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
