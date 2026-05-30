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
package me.predatorray.candybox.bookkeeper;

import java.util.List;
import me.predatorray.candybox.common.exception.StorageException;

/** A read view over a ledger. Reads are bounded by {@link #lastAddConfirmed()}. */
public interface ReadableLedger extends Ledger {

    /**
     * Reads a single entry.
     *
     * @param entryId entry id in {@code [0, lastAddConfirmed()]}
     * @return the entry
     * @throws StorageException if the entry id is out of range or the read fails
     */
    LedgerEntry read(long entryId);

    /**
     * Reads an inclusive range of entries.
     *
     * @param firstEntryId first entry id (inclusive)
     * @param lastEntryId  last entry id (inclusive); must be {@code <= lastAddConfirmed()}
     * @return the entries in order
     * @throws StorageException if the range is invalid or the read fails
     */
    List<LedgerEntry> readRange(long firstEntryId, long lastEntryId);
}
