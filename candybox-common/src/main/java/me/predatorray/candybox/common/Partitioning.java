/*
 * Copyright (c) 2026 the original author or authors.
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
package me.predatorray.candybox.common;

import java.nio.charset.StandardCharsets;
import me.predatorray.candybox.common.checksum.Crc32c;

/**
 * The Box hash-partitioning function, shared by servers (engine selection) and clients (routing) so
 * both always agree on which partition a key lives in. CRC32C over the key's UTF-8 bytes keeps it
 * deterministic across JVMs and architectures and dependency-free.
 *
 * <p>A Box's partition count is fixed at creation (see {@code BoxDescriptor}), so the mapping for a
 * given Box never changes.
 */
public final class Partitioning {

    private Partitioning() {
    }

    /** The partition of {@code key} in a Box with {@code partitionCount} partitions. */
    public static int partitionOf(String key, int partitionCount) {
        return partitionOf(key.getBytes(StandardCharsets.UTF_8), partitionCount);
    }

    /** The partition of a key given as UTF-8 bytes. */
    public static int partitionOf(byte[] keyUtf8, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive: " + partitionCount);
        }
        return (Crc32c.of(keyUtf8) & 0x7fffffff) % partitionCount;
    }
}
