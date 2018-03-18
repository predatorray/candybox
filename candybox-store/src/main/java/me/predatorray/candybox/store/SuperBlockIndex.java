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

import me.predatorray.candybox.ObjectKey;
import me.predatorray.candybox.store.util.ConcurrentUnidirectionalLinkedMap;
import me.predatorray.candybox.util.Validations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.*;

public class SuperBlockIndex {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ConcurrentUnidirectionalLinkedMap<ObjectKey, ObjectEntry> inMemoryMappings;

    public SuperBlockIndex(Path indexFilePath, boolean create, Executor indexPersistenceThreadPool, int capacity) {
        inMemoryMappings = new ConcurrentUnidirectionalLinkedMap<>(capacity);

        indexPersistenceThreadPool.execute(new IndexPersistenceDaemon());
    }

    public boolean put(ObjectKey objectKey, BlockLocation locationStored, short flags) {
        Validations.notNull(objectKey);
        Validations.notNull(locationStored);

        ObjectEntry indexedEntry = new ObjectEntry(locationStored, flags);
        return inMemoryMappings.put(objectKey, indexedEntry);
    }

    public BlockLocation queryLocation(ObjectKey objectKey) {
        Validations.notNull(objectKey);

        ObjectEntry entry = inMemoryMappings.get(objectKey);
        return entry == null ? null : entry.location;
    }

    private class IndexPersistenceDaemon implements Runnable {

        @Override
        public void run() {
            ObjectKey objectKey;
            ObjectEntry objectEntry;

            while (true) {
                try {
                    ConcurrentUnidirectionalLinkedMap.Entry<ObjectKey, ObjectEntry> next = inMemoryMappings.take();
                    objectKey = next.getKey();
                    objectEntry = next.getValue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("The index persistence daemon is stopped because of an interruption");
                    break;
                }

                // TODO write to the index file
            }
        }
    }

    private static class ObjectEntry {

        final BlockLocation location;
        final short flags;

        ObjectEntry(BlockLocation location, short flags) {
            this.location = location;
            this.flags = flags;
        }
    }
}
