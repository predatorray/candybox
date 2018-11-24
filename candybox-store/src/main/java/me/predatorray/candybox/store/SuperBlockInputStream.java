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
import me.predatorray.candybox.store.util.InputStreamWrapper;
import me.predatorray.candybox.util.EncodingUtils;
import me.predatorray.candybox.util.IOUtils;

import java.io.DataInputStream;
import java.io.IOException;

public class SuperBlockInputStream extends InputStreamWrapper<DataInputStream> {

    public SuperBlockInputStream(DataInputStream inputStream) {
        super(inputStream);
    }

    public ObjectKey readObjectKey() throws IOException {
        // object key size
        Short sizeOrNone = IOUtils.readShortOrNone(getInputStream());
        if (sizeOrNone == null) {
            return null;
        }

        int keySize = EncodingUtils.toUnsignedShort(sizeOrNone, true);
        if (keySize <= 0) {
            throw new MalformedBlockException("Non-positive key size: " + keySize);
        }

        // object key
        byte[] keyInBytes = new byte[keySize];
        getInputStream().readFully(keyInBytes);
        return new ObjectKey(keyInBytes);
    }
    
    public MagicNumber readMagicNumber() throws IOException {
        int value = getInputStream().readInt();
        return new MagicNumber(value);
    }

    public short readFlags() throws IOException {
        return getInputStream().readShort();
    }

    public BlockLocation readBlockLocation() throws IOException {
        long offset = getInputStream().readLong();
        long size = getInputStream().readLong();
        return new BlockLocation(offset, size);
    }

    public long readDataSize() throws IOException {
        return EncodingUtils.toUnsignedInt(getInputStream().readInt(), false);
    }

    public int readChecksum() throws IOException {
        return getInputStream().readInt();
    }
}
