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

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Path;

public class CandyBlockInputStream extends InputStream {

    private final Path superBlockPath;
    private final BlockLocation blockLocation;

    private final RandomAccessFile randomAccessFile;
    private final MagicNumber magicNumber;
    private final ObjectKey objectKey;
    private final short flags;
    private final long dataSize;

    private long bytesRead = 0;

    public CandyBlockInputStream(Path superBlockPath, BlockLocation blockLocation) throws IOException {
        this.superBlockPath = Validations.notNull(superBlockPath);
        this.blockLocation = Validations.notNull(blockLocation);

        RandomAccessFile raf = new RandomAccessFile(superBlockPath.toFile(), "r");
        raf.seek(blockLocation.getOffset());

        this.magicNumber = new MagicNumber(raf.readInt());
        if (!SuperBlock.DEFAULT_MAGIC_NUMBER.equals(magicNumber)) {
            throw new UnsupportedBlockFormatException(magicNumber);
        }

        int keySize = raf.readUnsignedShort();
        if (keySize <= 0) {
            throw new MalformedBlockException("Non-positive key size: " + keySize);
        }
        byte[] keyInBytes = new byte[keySize];
        raf.readFully(keyInBytes);
        this.objectKey = new ObjectKey(keyInBytes);

        this.flags = raf.readShort();

        this.dataSize = EncodingUtils.toUnsignedInt(raf.readInt(), false);
        if (this.dataSize < 0) {
            throw new MalformedBlockException("Negative key size: " + keySize);
        }

        this.randomAccessFile = raf;
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

    @Override
    public int read() throws IOException {
        if (remaining() <= 0) {
            return -1;
        }

        bytesRead += 1;
        return randomAccessFile.readByte();
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (b.length == 0) {
            return 0;
        }
        long remaining = remaining();
        if (remaining <= 0) {
            return -1;
        }

        int bytesToRead = (int) Math.min(remaining, b.length);
        randomAccessFile.readFully(b, 0, bytesToRead);
        bytesRead += bytesToRead;
        return bytesToRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        long remaining = remaining();
        if (remaining <= 0) {
            return -1;
        }

        int bytesToRead = (int) Math.min(remaining, len);
        randomAccessFile.readFully(b, off, bytesToRead);
        bytesRead += bytesToRead;
        return bytesToRead;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0L;
        }

        int bytesToSkip = (int) Math.min(Integer.MAX_VALUE, n);
        int bytesActuallySkipped = randomAccessFile.skipBytes(bytesToSkip);
        bytesRead += bytesActuallySkipped;
        return bytesActuallySkipped;
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.close();
    }

    @Override
    public String toString() {
        return superBlockPath + " " + blockLocation;
    }

    private long remaining() {
        return dataSize - bytesRead;
    }
}
