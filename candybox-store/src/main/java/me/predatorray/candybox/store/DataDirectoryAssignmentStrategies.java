package me.predatorray.candybox.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.util.Validations;

public class DataDirectoryAssignmentStrategies {

  public static DataDirectoryAssignmentStrategy roundRobin(List<SingleDataDirLocalShardManager> managers) {
    Validations.notEmpty(managers);
    return new RoundRobin(managers);
  }

  public static DataDirectoryAssignmentStrategy diskFreeSpaceFirst(List<SingleDataDirLocalShardManager> managers) {
    Validations.notEmpty(managers);
    return new DiskFreeSpaceFirst(managers);
  }

  private static class RoundRobin implements DataDirectoryAssignmentStrategy {

    private final List<SingleDataDirLocalShardManager> managers;
    private final int size;
    private final AtomicInteger counter = new AtomicInteger(0);

    RoundRobin(List<SingleDataDirLocalShardManager> managers) {
      this.managers = new ArrayList<>(managers);
      this.size = managers.size();
    }

    @Override
    public SingleDataDirLocalShardManager assign(String boxName, int offset) {
      while (true) {
        int current = counter.get();
        int next = (current + 1) % size;
        if (counter.compareAndSet(current, next)) {
          return managers.get(current);
        }
      }
    }
  }

  private static class DiskFreeSpaceFirst implements DataDirectoryAssignmentStrategy {

    private final List<SingleDataDirLocalShardManager> managers;

    DiskFreeSpaceFirst(List<SingleDataDirLocalShardManager> managers) {
      this.managers = managers;
    }

    @Override
    public SingleDataDirLocalShardManager assign(String boxName, int offset) {
      return Collections.max(managers, Comparator.comparingLong(m -> m.getDiskSpace().getFreeSpace()));
    }
  }
}
