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

import com.google.common.annotations.VisibleForTesting;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Predicate;
import me.predatorray.candybox.store.config.Configuration;
import me.predatorray.candybox.store.util.BackOffPolicy;
import me.predatorray.candybox.store.util.Retry;
import me.predatorray.candybox.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class FsLocalShard implements LocalShard {

    public static final String INDEX_FILE_SUFFIX = ".idx";
    public static final String BLOCK_FILE_SUFFIX = ".cbx";

    private static final Logger logger = LoggerFactory.getLogger(FsLocalShard.class);

    private final Path shardPath;
    private final String boxName;
    private final int offset;

    private final BackOffPolicy superBlockRecoveryBackOffPolicy;

    private final ReentrantReadWriteLock superBlockAndIndexGenLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock superBlockAndIndexGenReadLock = superBlockAndIndexGenLock.readLock();
    private final ReentrantReadWriteLock.WriteLock superBlockAndIndexGenWriteLock =
            superBlockAndIndexGenLock.writeLock();

    private final Object appendMonitor = new Object();

    @GuardedBy("superBlockAndIndexGenLock")
    private BigInteger generation;
    @GuardedBy("superBlockAndIndexGenLock")
    private SuperBlock block;
    @GuardedBy("superBlockAndIndexGenLock")
    private SuperBlockIndex index;

    static FsLocalShard restore(Path shardPath, String boxName, int offset,
                                Configuration configuration) throws IOException {
        Optional<Path> minBlockPathOpt = Files.list(shardPath)
            .filter(p -> p.endsWith(BLOCK_FILE_SUFFIX))
            .min(Comparator.comparing(FsLocalShard::superBlockGeneration));

        BigInteger currentGeneration;
        if (!minBlockPathOpt.isPresent()) {
            logger.debug("Since no super block file found with suffix '" + BLOCK_FILE_SUFFIX +
                "' under the shard directory: {}, ZERO is used as the current generation.", shardPath);
            currentGeneration = BigInteger.ZERO;
        } else {
            Path currentSuperBlockPath = minBlockPathOpt.get();
            currentGeneration = superBlockGeneration(currentSuperBlockPath);
            logger.debug("The current super block file ({} th generation) is found on path '{}'.", currentGeneration, currentSuperBlockPath);
        }

        try {
            Files.list(shardPath)
                .filter(new IsBlockOrIndexButNotOfGeneration(currentGeneration))
                .forEach(IOUtils.unchecked(Files::delete));
        } catch (UncheckedIOException unchecked) {
            throw unchecked.getCause();
        }
        return new FsLocalShard(shardPath, boxName, offset, currentGeneration, configuration);
    }

    FsLocalShard(Path shardPath, String boxName, int offset, BigInteger generation, Configuration configuration)
            throws IOException {
        this.shardPath = shardPath;
        this.boxName = boxName;
        this.offset = offset;
        this.generation = generation;

        Path idxFilePath = idxFilePath(shardPath, generation);
        Path cbxFilePath = cbxFilePath(shardPath, generation);

        block = SuperBlock.createIfNotExists(cbxFilePath);
        index = SuperBlockIndex.restoreSuperBlockIndex(idxFilePath, block, configuration.getIndexCapacity(),
                configuration.getIndexPersistenceThreadPool(), configuration.getIndexPersistenceBackOffPolicy());

        this.superBlockRecoveryBackOffPolicy = configuration.getSuperBlockRecoveryBackOffPolicy();
    }

    @Override
    public String boxName() {
        return boxName;
    }

    @Override
    public int offset() {
        return offset;
    }

    @Override
    public LocalShard.Snapshot takeSnapshot() {
        return new Snapshot();
    }

    @Override
    public SuperBlockIndex index() {
        return index;
    }

    @Override
    public SuperBlock block() {
        return block;
    }

    @VisibleForTesting
    BigInteger getGeneration() {
        return generation;
    }

    @Override
    public void append(CandyBlock candyBlock, AppendCallback callback) {
        synchronized (appendMonitor) {
            BlockLocation location;
            superBlockAndIndexGenReadLock.lock();
            try {
                try {
                    location = block.append(candyBlock);
                } catch (Throwable t) {
                    callback.onError(t);

                    try {
                        new Retry(() -> block.recover(index), superBlockRecoveryBackOffPolicy).runAndRetry();
                    } catch (InterruptedException e) {
                        logger.warn("The super block recovery is interrupted by a probably process termination. " +
                                "It will be re-scheduled upon restart.");
                    } catch (ExecutionException e) {
                        logger.error("The super block recovery is stopped because of an unexpected retry give-up.",
                                e.getCause());
                        // TODO: trigger a jvm shutdown or a stop-world self-healing
                        System.exit(2);
                    }
                    return;
                }

                try {
                    index.putInterruptibly(candyBlock.getObjectKey(), location, candyBlock.getFlags());
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        logger.warn("The index appending is interrupted by a probably process termination. " +
                                "It will be synced and recovered upon restart.");
                    } else {
                        logger.error("Failed to write the stored location of a candy block into the index due to " +
                                "an error, which may lead unexpected behaviors because of the inconsistency " +
                                "between the data and the index.", e);
                    }
                    callback.onError(e);
                    // TODO: trigger a jvm shutdown or a stop-world self-healing
                    System.exit(2);
                    return;
                }
            } finally {
                superBlockAndIndexGenReadLock.unlock();
            }

            callback.onCompleted(location);
        }
    }

    @Override
    public void close() throws IOException {
        superBlockAndIndexGenWriteLock.lock();
        try {
            IOUtils.closeSequentially(Arrays.asList(block, index));
        } finally {
            superBlockAndIndexGenWriteLock.unlock();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FsLocalShard that = (FsLocalShard) o;
        return offset == that.offset &&
                Objects.equals(shardPath, that.shardPath) &&
                Objects.equals(boxName, that.boxName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardPath, boxName, offset);
    }

    private class Snapshot implements LocalShard.Snapshot {

        SuperBlock block;
        SuperBlockIndex index;

        boolean closed = false;

        Snapshot() {
            superBlockAndIndexGenReadLock.lock();
            try {
                logger.debug("The {} th generation snapshot is taken.", FsLocalShard.this.generation);
                this.block = FsLocalShard.this.block;
                this.index = FsLocalShard.this.index;
            } finally {
                superBlockAndIndexGenReadLock.unlock();
            }
        }

        @Override
        public SuperBlockIndex index() {
            ensureNotClosed();
            return index;
        }

        @Override
        public SuperBlock block() {
            ensureNotClosed();
            return block;
        }

        @Override
        public void close() {
            ensureNotClosed();

            // TODO notify these instances can be closed if they were compressed
            this.block = null;
            this.index = null;

            this.closed = true;
        }

        private void ensureNotClosed() {
            if (closed) {
                throw new IllegalStateException("The snapshot has already been closed.");
            }
        }
    }

    private static BigInteger superBlockGeneration(Path superBlockPath) {
        String superBlockFileName = superBlockPath.getFileName().toString();
        if (!superBlockFileName.endsWith(BLOCK_FILE_SUFFIX)) {
            throw new IllegalArgumentException("The file name does not end with " + BLOCK_FILE_SUFFIX);
        }
        return new BigInteger(superBlockFileName.substring(0, superBlockFileName.length() - BLOCK_FILE_SUFFIX.length()));
    }

    private static Path idxFilePath(Path shardPath, BigInteger generation) {
        return shardPath.resolve(generation.toString() + INDEX_FILE_SUFFIX);
    }

    private static Path cbxFilePath(Path shardPath, BigInteger generation) {
        return shardPath.resolve(generation.toString() + BLOCK_FILE_SUFFIX);
    }

    private static class IsBlockOrIndexButNotOfGeneration implements Predicate<Path> {

        private final String generation;

        IsBlockOrIndexButNotOfGeneration(BigInteger generation) {
            this.generation = generation.toString();
        }

        @Override
        public boolean test(Path path) {
            String fileName = path.getFileName().toString();
            if (fileName.endsWith(INDEX_FILE_SUFFIX)) {
                return !generation.equals(fileName.substring(0, fileName.length() - INDEX_FILE_SUFFIX.length()));
            }
            if (fileName.endsWith(BLOCK_FILE_SUFFIX)) {
                return !generation.equals(fileName.substring(0, fileName.length() - BLOCK_FILE_SUFFIX.length()));
            }
            return false;
        }
    }
}
