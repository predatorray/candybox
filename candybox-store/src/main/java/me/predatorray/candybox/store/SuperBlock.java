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

import me.predatorray.candybox.MagicNumber;
import me.predatorray.candybox.ObjectFlags;
import me.predatorray.candybox.ObjectKey;
import me.predatorray.candybox.util.IOUtils;
import me.predatorray.candybox.util.Validations;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Optional;

public class SuperBlock extends AbstractCloseable {

    public static final String DEFAULT_MAGIC_NUMBER_STRING = "CBX0";
    public static final MagicNumber DEFAULT_MAGIC_NUMBER = new MagicNumber(DEFAULT_MAGIC_NUMBER_STRING);

    private static final long MAXIMUM_DATA_SIZE = (1L << 32) - 1L;
    private static final long DATA_SIZE_FIXED_LENGTH_IN_BYTES = Integer.BYTES;
    private static final long DATA_CHECKSUM_FIXED_LENGTH_IN_BYTES = Integer.BYTES;

    private final Path superBlockPath;
    private final IOUtils.Supplier<SuperBlockOutputStream> superBlockOutputSupplier;
    private SuperBlockOutputStream superBlockOutput;

    private long offset;
    private boolean corrupt = false;

    public static SuperBlock createIfNotExists(Path superBlockPath) throws IOException {
        Validations.notNull(superBlockPath);
        IOUtils.Supplier<SuperBlockOutputStream> superBlockOutputSupplier = () ->
                SuperBlockOutputStream.createAndAppend(superBlockPath);
        return new SuperBlock(superBlockPath, superBlockOutputSupplier);
    }

    SuperBlock(Path superBlockPath,
               IOUtils.Supplier<SuperBlockOutputStream> superBlockOutputSupplier) throws IOException {
        this.superBlockPath = superBlockPath;
        this.offset = size();
        this.superBlockOutputSupplier = superBlockOutputSupplier;
        this.superBlockOutput = this.superBlockOutputSupplier.get();
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
        Validations.notNull(magicNumber);
        Validations.notNull(objectKey);
        Validations.notNull(dataInput);
        Validations.that(dataSize, dataSize < MAXIMUM_DATA_SIZE && dataSize >= 0,
                "data size must be within the range [0, " + MAXIMUM_DATA_SIZE + ")");

        ensureBlockIsNotCorrupt();
        ensureNotClosed();

        try {
            // PART          || magic-number | object-key-size | object-key | flags | data-size | data | data-checksum
            // SIZE IN BYTES || 4            | 2               | var        | 2     | 4         | var  | 4
            superBlockOutput.writeMagicHeader(magicNumber);
            superBlockOutput.writeObjectKey(objectKey);
            superBlockOutput.writeFlags(flags);
            superBlockOutput.writeData(dataInput, dataSize);
            superBlockOutput.flush();
        } catch (IOException e) {
            corrupt = true;
            throw e;
        }

        long candySize = calculateBlockSize(objectKey, dataSize);
        offset += candySize;
        return new BlockLocation(offset - candySize, candySize);
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

        if (!this.corrupt) {
            return;
        }

        IOUtils.closeQuietly(this.superBlockOutput);

        Optional<BlockLocation> lastBlockLocationOpt = index.getLastBlockLocation();
        IOUtils.truncate(superBlockPath, lastBlockLocationOpt.map(BlockLocation::getNextOffset).orElse(0L));

        // reopen the superBlockOutput again
        this.superBlockOutput = this.superBlockOutputSupplier.get();

        this.offset = Files.size(superBlockPath);
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

    // Visible for Testing
    static long calculateBlockSize(ObjectKey objectKey, long dataSize) {
        return MagicNumber.FIXED_LENGTH_IN_BYTES +
                ObjectKey.OBJECT_KEY_SIZE_FIXED_LENGTH_IN_BYTES +
                ObjectFlags.FIXED_LENGTH_IN_BYTES +
                DATA_SIZE_FIXED_LENGTH_IN_BYTES +
                DATA_CHECKSUM_FIXED_LENGTH_IN_BYTES +
                objectKey.getSize() + dataSize;
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
