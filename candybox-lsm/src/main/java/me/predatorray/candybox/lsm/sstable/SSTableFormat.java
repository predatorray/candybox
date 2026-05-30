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
package me.predatorray.candybox.lsm.sstable;

/**
 * Shared constants for the SSTable on-ledger layout. An SSTable ledger is laid out as:
 *
 * <pre>
 *   entry 0 .. B-1   data blocks    (each block = a run of length-prefixed Mutations, key-ascending)
 *   entry B          bloom block    (serialized BloomFilter over all keys)
 *   entry B+1        index block    (per data block: lastKey + its entry id)
 *   entry [B+2]      range-del block (v2+, optional: serialized RangeTombstone list, by start)
 *   entry (=LAC)     footer         (magic, version, bloom/index/range-del entry ids, counts, keys)
 * </pre>
 *
 * <p>The footer is always the last entry, so a reader finds it at {@code lastAddConfirmed()} and from
 * there locates the index, bloom, and (v2+) the range-tombstone block. One block maps to one ledger
 * entry; data block size targets ~64 KiB.
 *
 * <p>Format version 1 has no range-del block; version 2 adds it (a table may also be range-only, with
 * zero data blocks). Readers accept both.
 */
final class SSTableFormat {

    static final int FOOTER_MAGIC = 0x53535442; // "SSTB"
    static final byte FORMAT_VERSION = 2;
    static final byte FORMAT_VERSION_NO_RANGE_DEL = 1;
    static final int DEFAULT_DATA_BLOCK_TARGET_BYTES = 64 * 1024;

    private SSTableFormat() {
    }
}
