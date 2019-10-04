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

import me.predatorray.candybox.store.config.Configuration;
import me.predatorray.candybox.store.util.DiskSpace;
import me.predatorray.candybox.store.util.FilenameSafeStringCodec;
import me.predatorray.candybox.util.IOUtils;
import me.predatorray.candybox.util.Validations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SingleDataDirLocalShardManager implements LocalShardManager {

    private static final Logger logger = LoggerFactory.getLogger(SingleDataDirLocalShardManager.class);

    private static final Pattern SHARD_FILENAME_REGEX = Pattern.compile("(.*)_([0-9]+)^");

    private final Configuration configuration;
    private final Path dataDirPath;
    private final FilenameSafeStringCodec filenameCodec;

    private final ConcurrentMap<ShardLocation, FsLocalShard> shardsByLocation = new ConcurrentHashMap<>();

    public SingleDataDirLocalShardManager(Path dataDirectoryPath, Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.dataDirPath = dataDirectoryPath;
        this.filenameCodec = configuration.getFilenameCodec();
    }

    @Override
    public FsLocalShard initialize(String boxName, int offset) throws CandyBlockIOException {
        Path shardPath = getShardPath(boxName, offset);

        if (Files.isDirectory(shardPath)) {
            return find(boxName, offset);
        }

        try {
            Files.createDirectories(shardPath);
        } catch (IOException e) {
            throw new CandyBlockIOException("Shard could not be initialized because the directory could not be created",
                    e);
        }

        return find(boxName, offset);
    }

    @Override
    public FsLocalShard find(String boxName, int offset) throws CandyBlockIOException {
        Objects.requireNonNull(boxName, "boxName must not be null");
        Validations.nonnegative(offset);

        ShardLocation shardLocation = new ShardLocation(boxName, offset);

        FsLocalShard localShard = shardsByLocation.get(shardLocation);
        if (localShard != null) {
            return localShard;
        }

        Path shardPath = getShardPath(boxName, offset);

        if (!Files.isDirectory(shardPath)) {
            logger.debug("Shard is not found because the path doesn't exist or it is not a directory: {}", shardPath);
            throw new ShardNotFoundException(boxName, offset);
        }

        return shardsByLocation.computeIfAbsent(shardLocation, IOUtils.unchecked(
                (it) -> FsLocalShard.restore(shardPath, boxName, offset, configuration)));
    }

    private Path getShardPath(String boxName, int offset) {
        String boxFilename = filenameCodec.encode(boxName);
        return dataDirPath.resolve(boxFilename + "_" + offset);
    }

    @Override
    public Collection<FsLocalShard> findAll() throws CandyBlockIOException {
        try {
            return Files.list(dataDirPath)
                    .map(this::readFromPath)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (UncheckedCandyBlockIOException e) {
            throw new CandyBlockIOException("io exception occurred while listing the data dir: " + dataDirPath,
                    e.getCause());
        } catch (IOException | UncheckedIOException e) {
            throw new CandyBlockIOException("io exception occurred while listing the data dir: " + dataDirPath, e);
        }
    }

    private FsLocalShard readFromPath(Path shardPath) {
        String shardFilename = shardPath.getFileName().toString();
        Matcher matcher = SHARD_FILENAME_REGEX.matcher(shardFilename);
        if (!matcher.matches()) {
            return null;
        }
        String encodedBoxName = matcher.group(1);
        int offset = Integer.parseInt(matcher.group(2));
        String boxName = filenameCodec.decode(encodedBoxName);
        try {
            return find(boxName, offset);
        } catch (CandyBlockIOException e) {
            throw new UncheckedCandyBlockIOException(e);
        }
    }

    public DiskSpace getDiskSpace() {
        return new DiskSpace(this.dataDirPath);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeSequentially(shardsByLocation.values());
    }

    private static class ShardLocation {

        String boxName;
        int offset;

        ShardLocation(String boxName, int offset) {
            this.boxName = boxName;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ShardLocation that = (ShardLocation) o;
            return offset == that.offset &&
                    Objects.equals(boxName, that.boxName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(boxName, offset);
        }
    }

    @Override
    public String toString() {
        return dataDirPath.toString();
    }
}
