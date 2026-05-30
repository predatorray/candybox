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

/**
 * The narrow storage SPI Candybox uses for ledgers. This is the <em>only</em> abstraction the LSM and
 * server build on; the real {@link me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore} and
 * the {@link me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore} both implement it and must
 * pass the same {@code LedgerStoreContract} suite.
 *
 * <p>The crucial distinction is {@link #openLedger(long)} (a passive read-only open) versus
 * {@link #recoverOpen(long)} (the fencing recover-open used on ownership handover): the latter seals
 * the ledger at a deterministic tail and prevents any prior writer from appending again.
 *
 * <p>Implementations are thread-safe.
 */
public interface LedgerStore extends AutoCloseable {

    /**
     * Creates a fresh, empty, writable ledger.
     *
     * @param config quorum and custom metadata
     * @return a writable handle (this caller is the sole writer)
     * @throws StorageException on failure
     */
    WritableLedger createLedger(LedgerConfig config);

    /**
     * Opens a ledger read-only <em>without</em> recovering or fencing it. Use for reading sealed
     * SSTable/Syrup/manifest ledgers whose tail is already settled.
     *
     * @param ledgerId the ledger id
     * @return a readable handle
     * @throws LedgerNotFoundException if no such ledger exists
     */
    ReadableLedger openLedger(long ledgerId);

    /**
     * Recover-opens a ledger: fences it (so any prior writer's subsequent appends fail), recovers and
     * seals it at a deterministic last-add-confirmed, and returns a readable handle. This is the
     * handover primitive for replaying a prior owner's WAL or manifest.
     *
     * @param ledgerId the ledger id
     * @return a readable handle over the recovered, sealed ledger
     * @throws LedgerNotFoundException if no such ledger exists
     */
    ReadableLedger recoverOpen(long ledgerId);

    /**
     * Deletes a ledger and its entries permanently.
     *
     * @param ledgerId the ledger id
     * @throws LedgerNotFoundException if no such ledger exists
     */
    void deleteLedger(long ledgerId);

    /**
     * Lists ledger ids known to the store. For the real backend this enumerates the cluster; callers
     * (GC) must filter to ledgers they own via custom metadata.
     *
     * @return ledger ids
     */
    List<Long> listLedgers();

    @Override
    void close();
}
