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

package me.predatorray.candybox.store.config;

import me.predatorray.candybox.store.DataDirectoryAssignmentStrategies;
import me.predatorray.candybox.store.DataDirectoryAssignmentStrategy;
import me.predatorray.candybox.store.SingleDataDirLocalShardManager;
import me.predatorray.candybox.store.util.BackOffPolicy;
import me.predatorray.candybox.store.util.Base36Codec;
import me.predatorray.candybox.store.util.ExponentialSleepBackOffPolicy;
import me.predatorray.candybox.store.util.FilenameSafeStringCodec;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public class DefaultConfiguration implements Configuration {

    public static final int DEFAULT_RPC_PORT = 50051;

    public static final FilenameSafeStringCodec DEFAULT_FILENAME_CODEC = new Base36Codec();

    private final Runtime runtime;

    public DefaultConfiguration() {
        this(Runtime.getRuntime());
    }

    DefaultConfiguration(Runtime runtime) {
        this.runtime = runtime;
    }

    @Override
    public int getAvailableProcessors() {
        return runtime.availableProcessors();
    }

    @Override
    public int getRpcPort() {
        return DEFAULT_RPC_PORT;
    }

    @Override
    public List<Path> getDataDirectoryPaths() {
        return Collections.singletonList(Paths.get("var/data/"));
    }

    @Override
    public FilenameSafeStringCodec getFilenameCodec() {
        return DEFAULT_FILENAME_CODEC;
    }

    @Override
    public int getIndexCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Executor getIndexPersistenceThreadPool() {
        return ForkJoinPool.commonPool();
    }

    @Override
    public BackOffPolicy getIndexPersistenceBackOffPolicy() {
        return new ExponentialSleepBackOffPolicy(1, 1000);
    }

    @Override
    public BackOffPolicy getSuperBlockRecoveryBackOffPolicy() {
        return new ExponentialSleepBackOffPolicy(1, 1000);
    }

    @Override
    public DataDirectoryAssignmentStrategy getDataDirectoryAssignmentStrategy(List<SingleDataDirLocalShardManager> managers) {
        return DataDirectoryAssignmentStrategies.roundRobin(managers);
    }

    @Override
    public void close() {
    }
}
