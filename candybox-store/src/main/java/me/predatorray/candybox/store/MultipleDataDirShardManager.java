package me.predatorray.candybox.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import me.predatorray.candybox.store.config.Configuration;
import me.predatorray.candybox.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class MultipleDataDirShardManager implements LocalShardManager {

  private static final Logger logger = LoggerFactory.getLogger(MultipleDataDirShardManager.class);

  private final List<SingleDataDirLocalShardManager> managers;
  private final ConcurrentMap<String, SingleDataDirLocalShardManager> managersByBoxName;
  private final DataDirectoryAssignmentStrategy dataDirectoryAssignmentStrategy;

  public MultipleDataDirShardManager(Configuration configuration) throws IOException {
    List<Path> dataDirectoryPaths = configuration.getDataDirectoryPaths();
    for (Path dataDirectoryPath : dataDirectoryPaths) {
      if (!Files.isDirectory(dataDirectoryPath)) {
        throw new FileNotFoundException("The data dir is either not a directory or has never been created yet: " +
            dataDirectoryPath);
      }
    }

    this.managersByBoxName = new ConcurrentHashMap<>();
    this.managers = dataDirectoryPaths.stream()
        .map(p -> new SingleDataDirLocalShardManager(p, configuration)).collect(Collectors.toList());
    for (SingleDataDirLocalShardManager manager : managers) {
      for (FsLocalShard fsLocalShard : manager.findAll()) {
        String boxName = fsLocalShard.boxName();
        if (managersByBoxName.containsKey(boxName)) {
          SingleDataDirLocalShardManager duplicateManager = managersByBoxName.get(boxName);
          throw new CandyBlockIOException("Found two box directories having the same box name '" + boxName + "': " + duplicateManager + ", " + manager + ".");
        }

        managersByBoxName.put(boxName, manager);
      }
    }

    dataDirectoryAssignmentStrategy = configuration.getDataDirectoryAssignmentStrategy(this.managers);
  }

  @Override
  public LocalShard initialize(String boxName, int offset) throws CandyBlockIOException {
    SingleDataDirLocalShardManager manager = dataDirectoryAssignmentStrategy.assign(boxName, offset);
    SingleDataDirLocalShardManager actualAssigned;
    try {
      actualAssigned = managersByBoxName.computeIfAbsent(boxName, it -> {
        IOUtils.unchecked(() -> manager.initialize(boxName, offset)).get();
        return manager;
      });
    } catch (UncheckedIOException e) {
      throw (CandyBlockIOException) e.getCause();
    }
    return actualAssigned.find(boxName, offset);
  }

  @Override
  public LocalShard find(String boxName, int offset) throws CandyBlockIOException {
    SingleDataDirLocalShardManager manager = managersByBoxName.get(boxName);
    if (manager == null) {
      logger.debug("Shard is not found for box {} at offset {} in the boxName-manager in-memory mapping.", boxName, offset);
      throw new ShardNotFoundException(boxName, offset);
    }
    return manager.find(boxName, offset);
  }

  @Override
  public Collection<? extends LocalShard> findAll() throws CandyBlockIOException {
    try {
      return managers.stream()
          .flatMap(IOUtils.unchecked(m -> m.findAll().stream()))
          .collect(Collectors.toList());
    } catch (UncheckedIOException e) {
      throw (CandyBlockIOException) e.getCause();
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeSequentially(managers);
  }
}
