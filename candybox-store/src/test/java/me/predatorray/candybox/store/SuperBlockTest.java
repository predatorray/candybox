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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

public class SuperBlockTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private SuperBlock sut;

    @Before
    public void setUpSuperBlock() throws Exception {
        File superBlockFile = temporaryFolder.newFile();
        sut = new SuperBlock(superBlockFile.toPath(), true);
    }

    @After
    public void tearDownSuperBlock() throws Exception {
        sut.close();
    }

    private void assertObjectEquals(ObjectKey expectedObjectKey, byte[] expectedData, BlockLocation storedLocation,
                                    short flags)
            throws IOException {
        try (CandyBlock candyBlock = sut.openCandyBlockAt(storedLocation)) {
            // magic number
            Assert.assertEquals(SuperBlock.DEFAULT_MAGIC_NUMBER, candyBlock.getMagicNumber());
            // object key
            Assert.assertEquals(expectedObjectKey, candyBlock.getObjectKey());

            // flags
            Assert.assertEquals(flags, candyBlock.getFlags());

            // checksum
            CRC32 crc32 = new CRC32();
            crc32.update(expectedData);
            int expectedChecksum = (int) crc32.getValue();
            Assert.assertEquals(expectedChecksum, candyBlock.getChecksum());

            // data size
            Assert.assertEquals((long) expectedData.length, candyBlock.getDataSize());

            // data
            ByteBuffer objectDataMap = candyBlock.getObjectDataMap();
            byte[] actualData = new byte[objectDataMap.remaining()];
            objectDataMap.get(actualData);
            Assert.assertArrayEquals(expectedData, actualData);
        }
    }

    @Test
    public void appendAndRead() throws Exception {
        final ObjectKey objectKey = new ObjectKey("foobar");
        final byte[] data = new byte[] { 1, 2, 3 };

        BlockLocation location;
        try (ByteArrayInputStream dataInputStream = new ByteArrayInputStream(data)) {
            location = sut.append(objectKey, dataInputStream, data.length);
        }
        Assert.assertEquals(new BlockLocation(0L, blockSize(objectKey, data)), location);

        assertObjectEquals(objectKey, data, location, ObjectFlags.NONE);
    }

    @Test
    public void appendTwoAndReadLastOne() throws Exception {
        final ObjectKey[] objectKeys = new ObjectKey[] {
                new ObjectKey("key0"),
                new ObjectKey("key1")
        };
        final byte[][] data = {
                new byte[] { 1, 2, 3 },
                new byte[] { 4, 5, 6 }
        };

        BlockLocation lastLocation = null;
        int expectedOffset = 0;
        for (int i = 0; i < objectKeys.length; i++) {
            ObjectKey objectKey = objectKeys[i];
            byte[] datum = data[i];

            try (ByteArrayInputStream dataInputStream = new ByteArrayInputStream(datum)) {
                lastLocation = sut.append(objectKey, dataInputStream, datum.length);
            }

            long expectedBlockSize = blockSize(objectKeys[i], data[i]);
            Assert.assertEquals(new BlockLocation(expectedOffset, expectedBlockSize),
                    lastLocation);
            expectedOffset += expectedBlockSize;
        }

        assertObjectEquals(objectKeys[objectKeys.length - 1], data[data.length - 1], lastLocation, ObjectFlags.NONE);
    }

    @Test
    public void dataOutOfRangeIsDiscarded() throws Exception {
        final ObjectKey objectKey = new ObjectKey("foobar");
        final byte[] data = new byte[] { 1, 2, 3 };
        final int discard = 2;
        final int dataSize = data.length - discard;

        BlockLocation location;
        try (ByteArrayInputStream dataInputStream = new ByteArrayInputStream(data)) {
            location = sut.append(objectKey, dataInputStream, dataSize);
        }
        Assert.assertEquals(new BlockLocation(0L, blockSize(objectKey, data) - discard), location);

        assertObjectEquals(objectKey, Arrays.copyOf(data, dataSize), location, ObjectFlags.NONE);
    }

    @Test
    public void appendingEmptyDataIsAllowed() throws Exception {
        final ObjectKey objectKey = new ObjectKey("empty");
        final byte[] empty = new byte[0];

        BlockLocation location;
        try (ByteArrayInputStream dataInputStream = new ByteArrayInputStream(empty)) {
            location = sut.append(objectKey, dataInputStream, 0);
        }

        Assert.assertEquals(new BlockLocation(0, blockSize(objectKey, empty)), location);
        assertObjectEquals(objectKey, empty, location, ObjectFlags.NONE);
    }

    @Test(expected = EOFException.class)
    public void passingArgumentExceedingActualSizeCausesEof() throws Exception {
        File superBlockFile = temporaryFolder.newFile();
        final ObjectKey objectKey = new ObjectKey("foobar");
        final byte[] data = new byte[] { 1, 2, 3 };
        final int dataSize = data.length + 1;

        try (SuperBlock sut = new SuperBlock(superBlockFile.toPath(), true)) {
            try (ByteArrayInputStream dataInputStream = new ByteArrayInputStream(data)) {
                sut.append(objectKey, dataInputStream, dataSize);
            }
        }
    }

    @Test
    public void flagsAreActuallyModified() throws Exception {
        final ObjectKey objectKey = new ObjectKey("foobar");
        final byte[] data = new byte[] { 1, 2, 3 };

        BlockLocation location;
        try (ByteArrayInputStream dataInputStream = new ByteArrayInputStream(data)) {
            location = sut.append(objectKey, dataInputStream, data.length);
        }

        final short deleted = ObjectFlags.DELETED;
        sut.changeFlagsOfCandyBlockAt(objectKey, location, deleted);

        assertObjectEquals(objectKey, data, location, deleted);
    }

    @Test
    public void flagsAreNotModifiedIfPassingTheSame() throws Exception {
        final ObjectKey objectKey = new ObjectKey("foobar");
        final byte[] data = new byte[] { 1, 2, 3 };

        BlockLocation location;
        try (ByteArrayInputStream dataInputStream = new ByteArrayInputStream(data)) {
            location = sut.append(objectKey, dataInputStream, data.length);
        }

        final short noneFlags = ObjectFlags.NONE;
        sut.changeFlagsOfCandyBlockAt(objectKey, location, noneFlags);

        assertObjectEquals(objectKey, data, location, noneFlags);
    }

    private static long blockSize(ObjectKey key, byte[] data) {
        // PART          || magic-number | object-key-size | object-key | flags | data-size | data | data-checksum
        // SIZE IN BYTES || 4            | 2               | var        | 2     | 4         | var  | 4
        return 16 + key.getSize() + data.length;
    }
}
