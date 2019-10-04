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

import me.predatorray.candybox.store.DataDirectoryAssignmentStrategy;
import me.predatorray.candybox.store.SingleDataDirLocalShardManager;
import me.predatorray.candybox.store.util.BackOffPolicy;
import me.predatorray.candybox.store.util.FilenameSafeStringCodec;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;

public interface Configuration extends Closeable {

    int getAvailableProcessors();

    int getRpcPort();

    List<Path> getDataDirectoryPaths();

    FilenameSafeStringCodec getFilenameCodec();

    int getIndexCapacity();

    Executor getIndexPersistenceThreadPool();

    BackOffPolicy getIndexPersistenceBackOffPolicy();

    BackOffPolicy getSuperBlockRecoveryBackOffPolicy();

    DataDirectoryAssignmentStrategy getDataDirectoryAssignmentStrategy(List<SingleDataDirLocalShardManager> managers);

    // TODO get CRC32 checksum
}
