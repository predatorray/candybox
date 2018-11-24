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
import me.predatorray.candybox.store.util.BackOffPolicy;
import me.predatorray.candybox.store.util.ConcurrentUnidirectionalLinkedMap;
import me.predatorray.candybox.store.util.IterativeExecutor;
import me.predatorray.candybox.store.util.IterativeExecutorService;
import me.predatorray.candybox.store.util.IterativeRunnable;
import me.predatorray.candybox.util.Validations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

public class SuperBlockIndex extends AbstractCloseable {

    public static final String DEFAULT_MAGIC_NUMBER_STRING = "CBI0";
    public static final MagicNumber DEFAULT_MAGIC_NUMBER = new MagicNumber(DEFAULT_MAGIC_NUMBER_STRING);

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ConcurrentUnidirectionalLinkedMap<ObjectKey, ObjectEntry> inMemoryMappings;
    private final BackOffPolicy indexPersistenceBackOffPolicy;

    private final SuperBlockOutputStream superBlockIndexOut;

    public SuperBlockIndex(Path indexFilePath, int capacity, Executor indexPersistenceThreadPool,
                           BackOffPolicy indexPersistenceBackOffPolicy) throws IOException {
        this(Validations.notNull(indexFilePath), Validations.positive(capacity),
                new IterativeExecutorService(Validations.notNull(indexPersistenceThreadPool)),
                        indexPersistenceBackOffPolicy);
    }

    // Visible for Testing
    SuperBlockIndex(Path indexFilePath, int capacity, IterativeExecutor iterativeExecutor,
                           BackOffPolicy indexPersistenceBackOffPolicy) throws IOException {
        this.indexPersistenceBackOffPolicy = Validations.notNull(indexPersistenceBackOffPolicy);
        if (Files.exists(indexFilePath)) {
            // load file into memory
            Map<ObjectKey, ObjectEntry> dataFromFile = new HashMap<>();
            try (DataInputStream dataIn = new DataInputStream(Files.newInputStream(indexFilePath));
                 SuperBlockInputStream in = new SuperBlockInputStream(dataIn)) {
                MagicNumber magicNumber = in.readMagicNumber();
                if (!DEFAULT_MAGIC_NUMBER.equals(magicNumber)) {
                    throw new UnsupportedBlockFormatException(magicNumber);
                }
                while (true) {
                    ObjectKey objectKey = in.readObjectKey();
                    if (objectKey == null) {
                        break;
                    }
                    ObjectEntry objectEntry = new ObjectEntry(in.readFlags(), in.readBlockLocation());
                    dataFromFile.put(objectKey, objectEntry);
                }
            }
            this.inMemoryMappings = new ConcurrentUnidirectionalLinkedMap<>(dataFromFile, capacity);

            this.superBlockIndexOut = new SuperBlockOutputStream(
                    new DataOutputStream(Files.newOutputStream(indexFilePath, StandardOpenOption.APPEND)));
            iterativeExecutor.submit(new IndexPersistenceIteration(superBlockIndexOut));
        } else {
            this.inMemoryMappings = new ConcurrentUnidirectionalLinkedMap<>(capacity);

            this.superBlockIndexOut = new SuperBlockOutputStream(
                    new DataOutputStream(Files.newOutputStream(indexFilePath,
                            StandardOpenOption.APPEND, StandardOpenOption.CREATE)));
            superBlockIndexOut.writeMagicHeader(DEFAULT_MAGIC_NUMBER);
            iterativeExecutor.submit(new IndexPersistenceIteration(superBlockIndexOut));
        }
    }

    public boolean put(ObjectKey objectKey, BlockLocation locationStored, short flags) {
        Validations.notNull(objectKey);
        Validations.notNull(locationStored);
        ensureNotClosed();

        ObjectEntry indexedEntry = new ObjectEntry(flags, locationStored);
        return inMemoryMappings.put(objectKey, indexedEntry);
    }

    public BlockLocation queryLocation(ObjectKey objectKey) {
        Validations.notNull(objectKey);
        ensureNotClosed();

        ObjectEntry entry = inMemoryMappings.get(objectKey);
        return entry == null ? null : entry.location;
    }

    @Override
    public void close() throws IOException {
        super.close();
        superBlockIndexOut.close();
    }

    private class IndexPersistenceIteration implements IterativeRunnable<IterationContextValue> {

        private final SuperBlockOutputStream outputStream;

        IndexPersistenceIteration(SuperBlockOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public IterationContextValue initialValue() {
            return new IterationContextValue(null, indexPersistenceBackOffPolicy.start());
        }

        @Override
        public void run(Context<IterationContextValue> context) {
            if (isClosed()) {
                logger.info("The index persistence daemon is stopped because the index was closed.");
                return;
            }

            IterationContextValue current = context.current();
            ConcurrentUnidirectionalLinkedMap.Entry<ObjectKey, ObjectEntry> next = current.next;
            BackOffPolicy.Context backOffCtx = current.backOffCtx;

            try {
                if (next == null) {
                    next = inMemoryMappings.take();
                } else {
                    logger.info("Retrying the failed persistence");
                }

                try {
                    outputStream.writeObjectKey(next.getKey());
                    ObjectEntry entry = next.getValue();
                    outputStream.writeFlags(entry.flags);
                    outputStream.writeBlockLocation(entry.location);
                    next = null;
                    backOffCtx = indexPersistenceBackOffPolicy.start();
                } catch (IOException e) {
                    logger.error("Failed to persist the index asynchronously. It will be tried again latter.", e);
                    indexPersistenceBackOffPolicy.backOff(backOffCtx);
                }

                context.cont(new IterationContextValue(next, backOffCtx));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                logger.info("The index persistence daemon is stopped because of an interruption");
            }
        }
    }

    private static class ObjectEntry {

        final BlockLocation location;
        final short flags;

        ObjectEntry(short flags, BlockLocation location) {
            this.location = location;
            this.flags = flags;
        }
    }

    private static class IterationContextValue {
        ConcurrentUnidirectionalLinkedMap.Entry<ObjectKey, ObjectEntry> next;
        BackOffPolicy.Context backOffCtx;

        IterationContextValue(ConcurrentUnidirectionalLinkedMap.Entry<ObjectKey, ObjectEntry> next,
                              BackOffPolicy.Context backOffCtx) {
            this.next = next;
            this.backOffCtx = backOffCtx;
        }
    }
}
