/*
 * Copyright (c) 2019 the original author or authors.
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
import me.predatorray.candybox.store.config.DefaultConfiguration;
import me.predatorray.candybox.store.testsupport.ByteBufferTestSupport;
import me.predatorray.candybox.store.util.BackOffPolicy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.math.BigInteger;
import java.util.concurrent.ForkJoinPool;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FsLocalShardTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testReadingFromASnapshot() throws Exception {
        final BigInteger generation = BigInteger.ZERO;
        File shardDir = temporaryFolder.newFolder();
        File superBlockFile = new File(shardDir, generation + FsLocalShard.BLOCK_FILE_SUFFIX);
        File indexFile = new File(shardDir, generation + FsLocalShard.INDEX_FILE_SUFFIX);

        ObjectKey objectKey = new ObjectKey("foobar");
        byte[] data = new byte[] {1, 2, 3};

        try (SuperBlock superBlock = SuperBlock.createIfNotExists(superBlockFile.toPath());
             SuperBlockIndex superBlockIndex = SuperBlockIndex.createSuperBlockIndex(indexFile.toPath(),
                     Integer.MAX_VALUE, ForkJoinPool.commonPool(), BackOffPolicy.IMMEDIATE)) {
            BlockLocation location = CandyBlockTest.makeCandy(superBlock, objectKey, data);
            assertTrue(superBlockIndex.put(objectKey, location, ObjectFlags.NONE));
        }

        try (FsLocalShard sut = new FsLocalShard(shardDir.toPath(), "boxFoobar", 0, generation,
                new DefaultConfiguration());
             LocalShard.Snapshot snapshot = sut.takeSnapshot()) {
            SuperBlockIndex index = snapshot.index();
            BlockLocation location = index.queryLocation(objectKey);
            SuperBlock block = snapshot.block();
            CandyBlock candyBlock = block.getCandyBlockAt(location);
            byte[] actualData = ByteBufferTestSupport.toByteArray(candyBlock.getObjectDataMaps());
            assertArrayEquals(data, actualData);
        }
    }

    @Test
    public void testMultipleGenerationsFound() throws Exception {
        final BigInteger firstGen = BigInteger.ZERO;
        final BigInteger nextGen = firstGen.add(BigInteger.ONE);
        File shardDir = temporaryFolder.newFolder();

        File firstGenBlockFile = new File(shardDir, firstGen + FsLocalShard.BLOCK_FILE_SUFFIX);
        File firstGenIdxFile = new File(shardDir, firstGen + FsLocalShard.INDEX_FILE_SUFFIX);
        assertTrue(firstGenBlockFile.createNewFile());
        assertTrue(firstGenIdxFile.createNewFile());

        File nextGenBlockFile = new File(shardDir, nextGen + FsLocalShard.BLOCK_FILE_SUFFIX);
        File nextGenIdxFile = new File(shardDir, nextGen + FsLocalShard.INDEX_FILE_SUFFIX);
        assertTrue(nextGenBlockFile.createNewFile());
        assertTrue(nextGenIdxFile.createNewFile());

        FsLocalShard restored = FsLocalShard.restore(shardDir.toPath(), "foobar", 0, new DefaultConfiguration());
        assertEquals(firstGen, restored.getGeneration());

        assertTrue(firstGenBlockFile.exists());
        assertTrue(firstGenIdxFile.exists());
        assertFalse(nextGenBlockFile.exists());
        assertFalse(nextGenIdxFile.exists());
    }
}