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
package me.predatorray.candybox.common.config;

/**
 * The distinct roles a BookKeeper ledger plays in Candybox. Each role has its own durability/quorum
 * defaults (see {@link QuorumConfig}) and lifecycle rules; keeping them separate in code prevents,
 * say, a throughput-tuned Syrup ledger from being created with WAL durability by accident.
 */
public enum LedgerRole {
    /** Write-ahead log; recovery source, strongest durability. */
    WAL,
    /** Append-only LSM metadata (manifest); truth of LSM state. */
    MANIFEST,
    /** Immutable sorted run produced by flush/compaction. */
    SSTABLE,
    /** Raw Candy bytes, chunked across entries. */
    SYRUP
}
