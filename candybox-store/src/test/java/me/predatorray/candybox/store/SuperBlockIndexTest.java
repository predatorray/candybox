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
import me.predatorray.candybox.store.testsupport.ManualExecutor;
import me.predatorray.candybox.store.util.BackOffPolicy;
import me.predatorray.candybox.store.util.IterativeExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class SuperBlockIndexTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public TestName testName = new TestName();

    private ManualExecutor executor;
    private SuperBlockIndex sut;

    private Path indexFilePath;

    @Before
    public void setUpSuperBlockIndex() throws Exception {
        File superBlockIndexFolder = temporaryFolder.newFolder();
        File superBlockIndexFile = new File(superBlockIndexFolder, testName.getMethodName() + ".idx");
        this.indexFilePath = superBlockIndexFile.toPath();
        this.executor = new ManualExecutor();
        sut = new SuperBlockIndex(indexFilePath, 100, new IterativeExecutorService(executor),
                BackOffPolicy.IMMEDIATE);
    }

    @After
    public void tearDownSuperBlock() throws Exception {
        sut.close();
    }

    @Test
    public void putAndRead() throws Exception {
        ObjectKey objectKey = new ObjectKey("foobar");
        BlockLocation location = new BlockLocation(100, 200);
        boolean put = sut.put(objectKey, location, ObjectFlags.NONE);
        assertTrue(put);

        BlockLocation locationReturned = sut.queryLocation(objectKey);
        assertEquals(location, locationReturned);
    }

    @Test
    public void indexCanBeRestoredAfterPersisted1() throws Exception {
        ObjectKey objectKey = new ObjectKey("foobar");
        BlockLocation location = new BlockLocation(100, 200);
        boolean put = sut.put(objectKey, location, ObjectFlags.NONE);
        assertTrue(put);

        executor.runNext();

        SuperBlockIndex restoredIndex = new SuperBlockIndex(indexFilePath, 100, new ManualExecutor(),
                BackOffPolicy.IMMEDIATE);
        BlockLocation locationReturned = restoredIndex.queryLocation(objectKey);
        assertEquals(location, locationReturned);
    }

    @Test
    public void indexCanBeRestoredAfterPersisted2() throws Exception {
        sut.put(new ObjectKey("dummy"), new BlockLocation(999, 999), ObjectFlags.DELETED);

        executor.runNext();

        ObjectKey objectKey = new ObjectKey("foobar");
        BlockLocation location = new BlockLocation(100, 200);
        boolean put = sut.put(objectKey, location, ObjectFlags.NONE);
        assertTrue(put);

        executor.runNext();

        SuperBlockIndex restoredIndex = new SuperBlockIndex(indexFilePath, 100, new ManualExecutor(),
                BackOffPolicy.IMMEDIATE);
        BlockLocation locationReturned = restoredIndex.queryLocation(objectKey);
        assertEquals(location, locationReturned);
    }

    @Test
    public void indexCanNotBeRestoredIfNotPersisted() throws Exception {
        ObjectKey objectKey = new ObjectKey("foobar");
        BlockLocation location = new BlockLocation(100, 200);
        boolean put = sut.put(objectKey, location, ObjectFlags.NONE);
        assertTrue(put);

        SuperBlockIndex restoredIndex = new SuperBlockIndex(indexFilePath, 100, new ManualExecutor(),
                BackOffPolicy.IMMEDIATE);
        assertNull(restoredIndex.queryLocation(objectKey));
    }
}
