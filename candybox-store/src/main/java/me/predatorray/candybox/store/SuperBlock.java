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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SuperBlock extends AbstractCloseable {

    public static final String DEFAULT_MAGIC_NUMBER_STRING = "CBX0";
    public static final MagicNumber DEFAULT_MAGIC_NUMBER = new MagicNumber(DEFAULT_MAGIC_NUMBER_STRING);
    // TODO magic footer

    private static final long MAXIMUM_DATA_SIZE = (1L << 32) - 1L;

    private final SuperBlockOutputStream superBlockOutput;
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
        this.superBlockOutput = new SuperBlockOutputStream(
                new DataOutputStream(Channels.newOutputStream(superBlockAppendChannel)));
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
        ensureNotClosed();

        try {
            superBlockOutput.writeMagicHeader(magicNumber);
            superBlockOutput.writeObjectKey(objectKey);
            superBlockOutput.writeFlags(flags);
            superBlockOutput.writeData(dataInput, dataSize);
            superBlockOutput.flush();
        } catch (IOException e) {
            corrupt = true;
            throw e;
        }

        long candySize = objectKey.getSize() + dataSize + 16;
        offset += candySize;
        return new BlockLocation(offset - candySize, candySize);
    }

    @Deprecated
    public MappedByteBuffer openMappedByteBuffer() throws IOException {
        ensureNotClosed();
        return superBlockAppendChannel.map(FileChannel.MapMode.READ_ONLY, 0, size());
    }

    @Deprecated
    public List<MappedByteBuffer> openMappedByteBuffer(BlockLocation location) throws IOException {
        Validations.notNull(location);
        ensureBlockIsWithinRange(location);
        ensureNotClosed();

        int bufferSize = (int) ((location.getLength() + Integer.MAX_VALUE - 1L) / Integer.MAX_VALUE);
        ArrayList<MappedByteBuffer> buffers = new ArrayList<>(bufferSize);
        long mapOffset = location.getOffset();
        long remaining = location.getLength();
        for (int i = 0; i < bufferSize; i++) {
            long mapLength = Math.min(remaining, Integer.MAX_VALUE);
            MappedByteBuffer buffer = superBlockAppendChannel.map(FileChannel.MapMode.READ_ONLY, mapOffset, mapLength);
            buffers.add(buffer);
            mapOffset += mapLength;
        }
        return buffers;
    }

    public CandyBlock getCandyBlockStartingAt(long startingOffset) throws IOException {
        return new CandyBlock(superBlockPath, startingOffset);
    }

    public CandyBlock getCandyBlockAt(BlockLocation location) throws IOException {
        Validations.notNull(location);
        ensureBlockIsWithinRange(location);
        ensureNotClosed();
        return new CandyBlock(superBlockPath, location);
    }

    public Iterator<CandyBlock> iterateCandyBlocks(long startingOffset) {
        ensureNotClosed();
        return new CandyBlockIterator(startingOffset);
    }

    public void changeFlagsOfCandyBlockAt(ObjectKey objectKey, BlockLocation location, short flags)
            throws IOException {
        Validations.notNull(objectKey);
        Validations.notNull(location);
        ensureBlockIsWithinRange(location);
        ensureNotClosed();

        long position = 6 + objectKey.getSize() + location.getOffset();
        try (FileChannel superBlockWriteChannel = FileChannel.open(superBlockPath, StandardOpenOption.WRITE,
                StandardOpenOption.READ)) {
            MappedByteBuffer flagsMap = superBlockWriteChannel.map(FileChannel.MapMode.READ_WRITE, position, 2);
            flagsMap.putShort(flags);
        }
    }

    public void recover(SuperBlockIndex index) throws IOException {
        ensureNotClosed();
        this.offset = Files.size(superBlockPath);
        // TODO
        this.corrupt = false;
    }

    public long size() throws IOException {
        ensureNotClosed();
        return Files.size(superBlockPath);
    }

    private void ensureBlockIsWithinRange(BlockLocation location) throws IOException {
        long superBlockSize = offset;
        if (location.isOutOfRange(superBlockSize)) {
            throw new IllegalArgumentException("The block locations " + location +
                    " is out of the super block range (size = " + superBlockSize + " bytes)");
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        superBlockOutput.close();
    }

    private class CandyBlockIterator implements Iterator<CandyBlock> {

        private long offset;
        private final long blockSize;

        CandyBlockIterator(long startingOffset) {
            this.offset = startingOffset;
            this.blockSize = SuperBlock.this.offset;
        }

        @Override
        public boolean hasNext() {
            return this.offset < this.blockSize;
        }

        @Override
        public CandyBlock next() throws UncheckedIOException {
            CandyBlock next;
            try {
                next = getCandyBlockStartingAt(offset);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            this.offset = next.getStartingOffsetOfNextBlock();
            return next;
        }
    }
}
