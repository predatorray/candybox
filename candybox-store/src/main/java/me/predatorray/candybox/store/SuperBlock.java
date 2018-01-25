/*
 * Copyright (c) 2017 the original author or authors.
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
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

public class SuperBlock implements Closeable {

    public static final String DEFAULT_MAGIC_NUMBER_STRING = "CBX0";
    public static final MagicNumber DEFAULT_MAGIC_NUMBER = new MagicNumber(DEFAULT_MAGIC_NUMBER_STRING);
    // TODO magic footer

    private static final int DEFAULT_BUFFER_SIZE = 2048;
    private static final long MAXIMUM_DATA_SIZE = (1L << 32) - 1L;

    private final DataOutputStream superBlockOutput;
    private final Path superBlockPath;
    private final FileChannel superBlockAppendChannel;

    private long offset;
    private boolean corrupt = false;

    public SuperBlock(Path superBlockPath, boolean create) throws IOException {
        this.superBlockPath = Validations.notNull(superBlockPath);

        OpenOption[] options;
        if (create) {
            options = new OpenOption[] {
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE
            };
        } else {
            options = new OpenOption[] {
                    StandardOpenOption.APPEND
            };
        }

        this.offset = size();

        this.superBlockAppendChannel = FileChannel.open(superBlockPath, options);
        this.superBlockOutput = new DataOutputStream(Channels.newOutputStream(superBlockAppendChannel));
    }

    private void ensureBlockIsNotCorrupt() throws CorruptBlockException {
        if (corrupt) {
            throw new CorruptBlockException();
        }
    }

    public BlockLocation append(ObjectKey objectKey, InputStream dataInput, long dataSize)
            throws IOException {
        return append(DEFAULT_MAGIC_NUMBER, objectKey, ObjectFlags.NONE, dataInput, dataSize);
    }

    public BlockLocation append(MagicNumber magicNumber, ObjectKey objectKey, short flags, InputStream dataInput,
                                long dataSize) throws IOException {
        // PART          || magic-number | object-key-size | object-key | flags | data-size | data | data-checksum
        // SIZE IN BYTES || 4            | 2               | var        | 2     | 4         | var  | 4
        Validations.notNull(magicNumber);
        Validations.notNull(objectKey);
        Validations.notNull(dataInput);
        Validations.that(dataSize, dataSize < MAXIMUM_DATA_SIZE && dataSize >= 0,
                "data size must be within the range [0, " + MAXIMUM_DATA_SIZE + ")");

        ensureBlockIsNotCorrupt();

        try {
            superBlockOutput.writeInt(magicNumber.toInteger());

            superBlockOutput.writeShort(objectKey.getSizeAsUnsignedShort());
            superBlockOutput.write(objectKey.getBinary());

            superBlockOutput.writeShort(flags);
            superBlockOutput.writeInt((int) dataSize); // TODO test ?

            CRC32 crc32 = new CRC32();

            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            long remaining = dataSize;
            while (remaining > 0) {
                int bytesToRead = (int) Math.min(remaining, buffer.length);
                int len = dataInput.read(buffer, 0, bytesToRead);
                if (len < 0) {
                    break;
                }
                superBlockOutput.write(buffer, 0, len);
                crc32.update(buffer, 0, len);
                remaining -= len;
            }

            if (remaining > 0) {
                throw new EOFException(
                        "the actual data size read from the input stream is less than the dataSize argument");
            }
            long value = crc32.getValue();
            superBlockOutput.writeInt((int) value);
            superBlockOutput.flush(); // FIXME inconsistent
        } catch (IOException e) {
            corrupt = true;
            throw e;
        }

        long candySize = objectKey.getSize() + dataSize + 16;
        offset += candySize;
        return new BlockLocation(offset - candySize, candySize);
    }

    public MappedByteBuffer openMappedByteBuffer() throws IOException {
        return superBlockAppendChannel.map(FileChannel.MapMode.READ_ONLY, 0, size());
    }

    public MappedByteBuffer openMappedByteBuffer(BlockLocation location) throws IOException {
        Validations.notNull(location);
        return superBlockAppendChannel.map(FileChannel.MapMode.READ_ONLY, location.getOffset(), location.getLength());
    }

    public CandyBlock openCandyBlockAt(BlockLocation location) throws IOException {
        Validations.notNull(location);
        return new CandyBlock(superBlockPath, location);
    }

    @Deprecated
    public CandyBlockInputStream openInputStreamOfCandyBlockAt(BlockLocation location) throws IOException {
        Validations.notNull(location);
        long size = size();
        if (location.isOutOfRange(size)) {
            throw new IllegalArgumentException("the location " + location + " is out of the superblock size " + size);
        }
        return new CandyBlockInputStream(superBlockPath, location);
    }

    public long size() throws IOException {
        return Files.size(superBlockPath);
    }

    @Override
    public void close() throws IOException {
        superBlockOutput.close();
    }
}
