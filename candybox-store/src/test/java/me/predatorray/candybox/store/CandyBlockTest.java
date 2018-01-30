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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

public class CandyBlockTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private SuperBlock superBlock;
    private Path superBlockPath;

    @Before
    public void setUpFixtures() throws Exception {
        File superBlockFile = temporaryFolder.newFile();
        superBlockPath = superBlockFile.toPath();
        superBlock = new SuperBlock(superBlockPath, true);
    }

    @After
    public void tearDownSuperBlock() throws Exception {
        superBlock.close();
    }

    private BlockLocation makeCandy(ObjectKey objectKey, byte[] data) throws IOException {
        BlockLocation location;
        try (ByteArrayInputStream dataInputStream = new ByteArrayInputStream(data)) {
            location = superBlock.append(objectKey, dataInputStream, data.length);
        }
        return location;
    }

    @Test
    public void multipleObjectDataMapsAreReturned() throws Exception {
        final ObjectKey objectKey = new ObjectKey("foobar");
        final byte[] data = new byte[] { 1, 2, 3, 4 };
        BlockLocation location = makeCandy(objectKey, data);

        CandyBlock candyBlock = new CandyBlock(superBlockPath, location, 2);
        List<ByteBuffer> objectDataMaps = candyBlock.getObjectDataMaps();
        Assert.assertEquals(2, objectDataMaps.size());

        byte[] actualData = new byte[4];
        int actualDataOffset = 0;
        for (ByteBuffer objectDataMap : objectDataMaps) {
            Assert.assertEquals(2, objectDataMap.remaining());
            objectDataMap.get(actualData, actualDataOffset, 2);
            actualDataOffset += 2;
        }

        Assert.assertArrayEquals(data, actualData);
    }
}