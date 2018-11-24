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

import me.predatorray.candybox.ObjectFlags;
import me.predatorray.candybox.ObjectKey;
import me.predatorray.candybox.util.Validations;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CandyBlock implements Closeable {

    public static final int DATA_SIZE_FIXED_LENGTH_IN_BYTES = 4;

    private final Path superBlockPath;
    private final BlockLocation blockLocation;

    private final List<? extends ByteBuffer> objectDataMaps;

    private final MagicNumber magicNumber;
    private final ObjectKey objectKey;
    private final short flags;
    private final long dataSize;
    private final int checksum;

    CandyBlock(Path superBlockPath, BlockLocation blockLocation) throws IOException {
        this(superBlockPath, blockLocation, Integer.MAX_VALUE);
    }

    CandyBlock(Path superBlockPath, BlockLocation blockLocation, int maximumByteBufferSize) throws IOException {
        this.superBlockPath = Validations.notNull(superBlockPath);
        this.blockLocation = Validations.notNull(blockLocation);
        Validations.positive(maximumByteBufferSize);

        long startOffset = blockLocation.getOffset();

        long dataBlockOffset;
        try (FileChannel dataBlockChannel = FileChannel.open(superBlockPath, StandardOpenOption.READ);) {
            try (InputStream fileChannelIn = Channels.newInputStream(dataBlockChannel.position(startOffset));
                 SuperBlockInputStream input = new SuperBlockInputStream(new DataInputStream(fileChannelIn))) {
                // magic number
                this.magicNumber = input.readMagicNumber();
                if (!SuperBlock.DEFAULT_MAGIC_NUMBER.equals(magicNumber)) {
                    throw new UnsupportedBlockFormatException(magicNumber);
                }

                this.objectKey = input.readObjectKey();
                this.flags = input.readFlags();
                this.dataSize = input.readDataSize();
                if (this.dataSize < 0) {
                    throw new MalformedBlockException("Negative key size: " + this.dataSize);
                }

                dataBlockOffset = startOffset + MagicNumber.FIXED_LENGTH_IN_BYTES +
                        ObjectKey.OBJECT_KEY_SIZE_FIXED_LENGTH_IN_BYTES + this.objectKey.getSize() +
                        ObjectFlags.FIXED_LENGTH_IN_BYTES + DATA_SIZE_FIXED_LENGTH_IN_BYTES;
                long checksumOffset = dataBlockOffset + dataSize;

                try (InputStream fileChecksumIn = Channels.newInputStream(dataBlockChannel.position(checksumOffset));
                     SuperBlockInputStream checksumInput = new SuperBlockInputStream(
                             new DataInputStream(fileChecksumIn))) {
                    this.checksum = checksumInput.readChecksum();

                    if (this.dataSize == 0) {
                        objectDataMaps = Collections.emptyList();
                    } else {
                        int objectDataMapSize = (int) ((dataSize + maximumByteBufferSize - 1L) / maximumByteBufferSize);
                        ArrayList<MappedByteBuffer> objectDataMaps = new ArrayList<>(objectDataMapSize);
                        long mapOffset = dataBlockOffset;
                        long remaining = dataSize;
                        for (int i = 0; i < objectDataMapSize; i++) {
                            long mapLength = Math.min(remaining, maximumByteBufferSize);
                            MappedByteBuffer buffer = dataBlockChannel.map(FileChannel.MapMode.READ_ONLY, mapOffset,
                                    mapLength);
                            objectDataMaps.add(buffer);
                            mapOffset += mapLength;
                            remaining -= mapLength;
                        }
                        this.objectDataMaps = Collections.unmodifiableList(objectDataMaps);
                    }
                }
            }
        }
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

    public List<ByteBuffer> getObjectDataMaps() {
        if (objectDataMaps.isEmpty()) {
            return Collections.emptyList();
        }

        List<ByteBuffer> duplication = new ArrayList<>(objectDataMaps.size());
        for (ByteBuffer objectDataMap : objectDataMaps) {
            duplication.add(objectDataMap.duplicate());
        }
        return duplication;
    }

    @Override
    public String toString() {
        return superBlockPath + " " + blockLocation;
    }

    @Deprecated
    @Override
    public void close() throws IOException {
    }
}
