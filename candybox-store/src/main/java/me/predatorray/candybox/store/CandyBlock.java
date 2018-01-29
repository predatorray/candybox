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

import me.predatorray.candybox.ObjectKey;
import me.predatorray.candybox.util.EncodingUtils;
import me.predatorray.candybox.util.Validations;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class CandyBlock implements Closeable {

    private final Path superBlockPath;
    private final BlockLocation blockLocation;

    private final FileChannel blockReadChannel;

    private final ByteBuffer objectDataMap;

    private final MagicNumber magicNumber;
    private final ObjectKey objectKey;
    private final short flags;
    private final long dataSize;
    private final int checksum;

    public CandyBlock(Path superBlockPath, BlockLocation blockLocation) throws IOException {
        this.superBlockPath = Validations.notNull(superBlockPath);
        this.blockLocation = Validations.notNull(blockLocation);

        this.blockReadChannel = FileChannel.open(superBlockPath, StandardOpenOption.READ);

        // FIXME mmap size greater than Integer.MAX_VALUE
        MappedByteBuffer candyBlockMemoryMap = blockReadChannel.map(FileChannel.MapMode.READ_ONLY,
                blockLocation.getOffset(), blockLocation.getLength());

        this.magicNumber = new MagicNumber(candyBlockMemoryMap.getInt());
        if (!SuperBlock.DEFAULT_MAGIC_NUMBER.equals(magicNumber)) {
            throw new UnsupportedBlockFormatException(magicNumber);
        }

        int keySize = EncodingUtils.toUnsignedShort(candyBlockMemoryMap.getShort(), true);
        if (keySize <= 0) {
            throw new MalformedBlockException("Non-positive key size: " + keySize);
        }
        byte[] keyInBytes = new byte[keySize];
        candyBlockMemoryMap.get(keyInBytes);
        this.objectKey = new ObjectKey(keyInBytes);

        this.flags = candyBlockMemoryMap.getShort();

        this.dataSize = EncodingUtils.toUnsignedInt(candyBlockMemoryMap.getInt(), false);
        if (this.dataSize < 0) {
            throw new MalformedBlockException("Negative key size: " + keySize);
        }

        int dataChecksumOffset = (int) (12 + keySize + dataSize); // FIXME overflow
        this.checksum = candyBlockMemoryMap.getInt(dataChecksumOffset);

        candyBlockMemoryMap.limit(dataChecksumOffset);
        this.objectDataMap = candyBlockMemoryMap.slice();
    }

    public MagicNumber getMagicNumber() {
        return magicNumber;
    }

    public ObjectKey getObjectKey() {
        return objectKey;
    }

    public short getFlags() {
        return flags;
    }

    public long getDataSize() {
        return dataSize;
    }

    public int getChecksum() {
        return checksum;
    }

    public ByteBuffer getObjectDataMap() {
        return objectDataMap;
    }

    @Override
    public String toString() {
        return superBlockPath + " " + blockLocation;
    }

    @Override
    public void close() throws IOException {
        blockReadChannel.close();
    }
}
