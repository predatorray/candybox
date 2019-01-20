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
import me.predatorray.candybox.store.util.OutputStreamWrapper;
import me.predatorray.candybox.util.IOUtils;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

public class SuperBlockOutputStream extends OutputStreamWrapper<DataOutputStream> {

    private static final int DEFAULT_WRITING_DATA_BUFFER_SIZE = 2048;

    public static SuperBlockOutputStream createAndAppend(Path superBlockPath) throws IOException {
        return new SuperBlockOutputStream(Files.newOutputStream(superBlockPath,
                StandardOpenOption.APPEND, StandardOpenOption.CREATE));
    }

    public SuperBlockOutputStream(OutputStream outputStream) {
        super(IOUtils.toDataOutputStream(outputStream));
    }

    public void writeMagicHeader(MagicNumber magicNumber) throws IOException {
        getOutputStream().writeInt(magicNumber.toInteger());
    }

    public void writeObjectKey(ObjectKey objectKey) throws IOException {
        getOutputStream().writeShort(objectKey.getSizeAsUnsignedShort());
        getOutputStream().write(objectKey.getBinary());
    }

    public void writeFlags(short flags) throws IOException {
        getOutputStream().writeShort(flags);
    }

    public void writeBlockLocation(BlockLocation blockLocation) throws IOException {
        getOutputStream().writeLong(blockLocation.getOffset());
        getOutputStream().writeLong(blockLocation.getLength());
    }

    public void writeData(InputStream dataInput, long dataSize) throws IOException {
        getOutputStream().writeInt((int) dataSize);

        CRC32 crc32 = new CRC32();

        byte[] buffer = new byte[DEFAULT_WRITING_DATA_BUFFER_SIZE];
        long remaining = dataSize;
        while (remaining > 0) {
            int bytesToRead = (int) Math.min(remaining, buffer.length);
            int len = dataInput.read(buffer, 0, bytesToRead);
            if (len < 0) {
                break;
            }
            getOutputStream().write(buffer, 0, len);
            crc32.update(buffer, 0, len);
            remaining -= len;
        }

        if (remaining > 0) {
            throw new EOFException(
                    "the actual data size read from the input stream is less than the dataSize argument");
        }
        long value = crc32.getValue();
        getOutputStream().writeInt((int) value);
    }
}
