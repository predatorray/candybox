package me.predatorray.candybox.store.util;

import java.io.File;
import java.nio.file.Path;

public class DiskSpace {

  private final File file;

  public DiskSpace(Path path) {
    this.file = path.toFile();
  }

  public long getFreeSpace() {
    return this.file.getFreeSpace();
  }

  public long getUsableSpace() {
    return this.file.getUsableSpace();
  }

  public long getTotalSpace() {
    return this.file.getTotalSpace();
  }
}
