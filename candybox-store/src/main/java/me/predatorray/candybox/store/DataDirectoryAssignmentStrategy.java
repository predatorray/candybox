package me.predatorray.candybox.store;

public interface DataDirectoryAssignmentStrategy {

  SingleDataDirLocalShardManager assign(String boxName, int offset);
}
