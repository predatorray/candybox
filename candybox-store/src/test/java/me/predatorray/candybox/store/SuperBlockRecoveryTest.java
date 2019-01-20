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
import me.predatorray.candybox.store.testsupport.ByteBufferTestSupport;
import me.predatorray.candybox.store.testsupport.ExceptionalOutputStream;
import me.predatorray.candybox.store.testsupport.ManualExecutor;
import me.predatorray.candybox.store.util.BackOffPolicy;
import me.predatorray.candybox.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static me.predatorray.candybox.store.testsupport.AssertExtension.assertThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SuperBlockRecoveryTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Path superBlockPath;
    private ManualExecutor executor;
    private SuperBlockIndex superBlockIndex;

    @Before
    public void setSuperBlockPathAndIndex() throws Exception {
        this.superBlockPath = temporaryFolder.newFile().toPath();
        Path superBlockIndexPath = temporaryFolder.newFile().toPath();

        this.executor = new ManualExecutor();
        this.superBlockIndex = SuperBlockIndex.createSuperBlockIndex(superBlockIndexPath, 100, executor,
                BackOffPolicy.IMMEDIATE);
    }

    @After
    public void tearDownIndex() throws Exception {
        this.superBlockIndex.close();
    }

    private BlockLocation storeAndIndex(SuperBlock superBlock, ObjectKey objectKey, byte[] data) throws IOException {
        BlockLocation location;
        try (ByteArrayInputStream dataIn = new ByteArrayInputStream(data)) {
            location = superBlock.append(objectKey, dataIn, data.length);
        }
        superBlockIndex.put(objectKey, location, ObjectFlags.NONE);
        executor.runNext();
        return location;
    }

    @Test
    public void nothingIsLeftIfFirstAppendFails() throws Exception {
        final ObjectKey objectKey = new ObjectKey("foobar");
        final byte[] data = new byte[] { 1, 2, 3 };
        int firstBlockLength = 1;

        IOException expectedEx = new IOException();

        try (SuperBlock sut = new SuperBlock(superBlockPath,
                new BrokenAndRecovered(superBlockPath, firstBlockLength, expectedEx))) {
            assertThrows(expectedEx, () -> storeAndIndex(sut, objectKey, data));

            sut.recover(superBlockIndex);

            // the block file is empty
            assertEquals(0L, Files.size(superBlockPath));
        }
    }

    @Test
    public void dataAppendedBeforeAreRestoredAfterFailure() throws Exception {
        final ObjectKey objectKey = new ObjectKey("foobar");
        final byte[] data = new byte[] { 1, 2, 3 };
        int firstBlockLength = (int) SuperBlock.calculateBlockSize(objectKey, data.length);
        int brokenOffset = firstBlockLength + 2;

        final ObjectKey brokenObjectKey = new ObjectKey("broken");

        IOException expectedEx = new IOException();

        try (SuperBlock sut = new SuperBlock(superBlockPath,
                new BrokenAndRecovered(superBlockPath, brokenOffset, expectedEx))) {
            BlockLocation storedLocOfFirstObj = storeAndIndex(sut, objectKey, data);
            assertThrows(expectedEx, () -> storeAndIndex(sut, brokenObjectKey, data));

            sut.recover(superBlockIndex);

            CandyBlock storedCb = sut.getCandyBlockAt(storedLocOfFirstObj);
            byte[] read = ByteBufferTestSupport.toByteArray(storedCb.getObjectDataMaps());
            assertArrayEquals(data, read);

            assertEquals(firstBlockLength, Files.size(superBlockPath));
        }
    }

    private static class BrokenAndRecovered implements IOUtils.Supplier<SuperBlockOutputStream> {

        private boolean first = true;
        private final Path superBlockPath;
        private final int brokenOffset;
        private final IOException exceptionThrownOnOffset;

        BrokenAndRecovered(Path superBlockPath, int brokenOffset, IOException exceptionThrownOnOffset) {
            this.superBlockPath = superBlockPath;
            this.brokenOffset = brokenOffset;
            this.exceptionThrownOnOffset = exceptionThrownOnOffset;
        }

        @Override
        public SuperBlockOutputStream get() throws IOException {
            if (first) {
                first = false;
                return new SuperBlockOutputStream(new ExceptionalOutputStream(
                        Files.newOutputStream(superBlockPath,
                                StandardOpenOption.APPEND, StandardOpenOption.CREATE),
                        brokenOffset, exceptionThrownOnOffset));
            }
            return SuperBlockOutputStream.createAndAppend(superBlockPath);
        }
    }
}
