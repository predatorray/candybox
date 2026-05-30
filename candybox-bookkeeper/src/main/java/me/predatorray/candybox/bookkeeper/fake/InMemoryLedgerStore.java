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
package me.predatorray.candybox.bookkeeper.fake;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import me.predatorray.candybox.bookkeeper.Ledger;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerNotFoundException;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.StorageException;

/**
 * An in-memory {@link LedgerStore} fake that deliberately models the <em>adversarial</em> semantics
 * that make Candybox's hard tests meaningful — not just a happy path:
 *
 * <ul>
 *   <li><b>Sealing / LAC.</b> A ledger seals on {@code close} or {@code recover-open}; reads are
 *       bounded by the recovered last-add-confirmed.</li>
 *   <li><b>Recover-open fencing.</b> {@link #recoverOpen(long)} bumps a fencing epoch and seals the
 *       ledger, so a prior writer's {@link WritableLedger#append(byte[])} fails with
 *       {@link FencedException} — exactly the zombie-owner defense.</li>
 *   <li><b>Ack-quorum.</b> {@link #setAvailableBookies(int)} injects bookie loss; create requires the
 *       ensemble and append requires the ack-quorum to be satisfiable, else {@link StorageException}.</li>
 * </ul>
 *
 * <p>Thread-safe. Entry payloads are defensively copied in and out so callers cannot mutate state.
 */
public final class InMemoryLedgerStore implements LedgerStore {

    private final ConcurrentMap<Long, FakeLedger> ledgers = new ConcurrentHashMap<>();
    private final AtomicLong idGenerator = new AtomicLong(0);
    private volatile int availableBookies = Integer.MAX_VALUE;
    private volatile boolean storeClosed = false;

    /** Sets how many bookies are currently reachable, to inject quorum-loss failures. */
    public void setAvailableBookies(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("availableBookies must be >= 0");
        }
        this.availableBookies = count;
    }

    private void ensureOpen() {
        if (storeClosed) {
            throw new StorageException("LedgerStore is closed");
        }
    }

    private FakeLedger require(long ledgerId) {
        FakeLedger l = ledgers.get(ledgerId);
        if (l == null) {
            throw new LedgerNotFoundException(ledgerId);
        }
        return l;
    }

    @Override
    public WritableLedger createLedger(LedgerConfig config) {
        ensureOpen();
        if (availableBookies < config.quorum().ensembleSize()) {
            throw new StorageException("Cannot create ledger: only " + availableBookies
                    + " bookie(s) available, ensemble requires " + config.quorum().ensembleSize());
        }
        long id = idGenerator.getAndIncrement();
        FakeLedger ledger = new FakeLedger(id, config);
        ledgers.put(id, ledger);
        return new WritableHandle(ledger, ledger.epoch);
    }

    @Override
    public ReadableLedger openLedger(long ledgerId) {
        ensureOpen();
        return new ReadableHandle(require(ledgerId));
    }

    @Override
    public ReadableLedger recoverOpen(long ledgerId) {
        ensureOpen();
        FakeLedger ledger = require(ledgerId);
        ledger.fenceAndSeal();
        return new ReadableHandle(ledger);
    }

    @Override
    public void deleteLedger(long ledgerId) {
        ensureOpen();
        if (ledgers.remove(ledgerId) == null) {
            throw new LedgerNotFoundException(ledgerId);
        }
    }

    @Override
    public List<Long> listLedgers() {
        ensureOpen();
        List<Long> ids = new ArrayList<>(ledgers.keySet());
        ids.sort(Long::compareTo);
        return ids;
    }

    @Override
    public void close() {
        storeClosed = true;
    }

    // -------------------------------------------------------------------------------------------
    // Internal state and handles
    // -------------------------------------------------------------------------------------------

    /** Mutable per-ledger state; all access synchronized on the instance. */
    private static final class FakeLedger {
        private final long id;
        private final LedgerConfig config;
        private final List<byte[]> entries = new ArrayList<>();
        private boolean sealed = false;
        private int epoch = 0;

        FakeLedger(long id, LedgerConfig config) {
            this.id = id;
            this.config = config;
        }

        synchronized long lac() {
            return entries.size() - 1L;
        }

        synchronized boolean sealed() {
            return sealed;
        }

        synchronized void seal() {
            sealed = true;
        }

        synchronized void fenceAndSeal() {
            sealed = true;
            epoch++;
        }

        synchronized long append(int writerEpoch, int availableBookies, byte[] data) {
            if (sealed) {
                throw new FencedException("Ledger " + id + " is sealed; appends are rejected");
            }
            if (writerEpoch != epoch) {
                throw new FencedException("Ledger " + id + " has been fenced (writer epoch "
                        + writerEpoch + " != current " + epoch + ")");
            }
            if (availableBookies < config.quorum().ackQuorum()) {
                throw new StorageException("Append to ledger " + id + " failed: ack-quorum of "
                        + config.quorum().ackQuorum() + " not met with " + availableBookies
                        + " bookie(s)");
            }
            entries.add(data);
            return entries.size() - 1L;
        }

        synchronized byte[] read(long entryId) {
            if (entryId < 0 || entryId > lac()) {
                throw new StorageException("Entry " + entryId + " out of range [0, " + lac()
                        + "] for ledger " + id);
            }
            return entries.get((int) entryId).clone();
        }
    }

    private abstract class BaseHandle implements Ledger {
        final FakeLedger ledger;

        BaseHandle(FakeLedger ledger) {
            this.ledger = ledger;
        }

        @Override
        public long ledgerId() {
            return ledger.id;
        }

        @Override
        public long lastAddConfirmed() {
            return ledger.lac();
        }

        @Override
        public boolean isSealed() {
            return ledger.sealed();
        }

        @Override
        public Map<String, byte[]> customMetadata() {
            return ledger.config.customMetadata();
        }
    }

    private class ReadableHandle extends BaseHandle implements ReadableLedger {
        ReadableHandle(FakeLedger ledger) {
            super(ledger);
        }

        @Override
        public LedgerEntry read(long entryId) {
            return new LedgerEntry(entryId, ledger.read(entryId));
        }

        @Override
        public List<LedgerEntry> readRange(long firstEntryId, long lastEntryId) {
            if (firstEntryId < 0 || lastEntryId < firstEntryId) {
                throw new StorageException("Invalid read range [" + firstEntryId + ", " + lastEntryId
                        + "]");
            }
            List<LedgerEntry> out = new ArrayList<>();
            for (long e = firstEntryId; e <= lastEntryId; e++) {
                out.add(read(e));
            }
            return out;
        }

        @Override
        public void close() {
            // Read handles hold no resources in the fake.
        }
    }

    private final class WritableHandle extends ReadableHandle implements WritableLedger {
        private final int writerEpoch;

        WritableHandle(FakeLedger ledger, int writerEpoch) {
            super(ledger);
            this.writerEpoch = writerEpoch;
        }

        @Override
        public long append(byte[] data) {
            return ledger.append(writerEpoch, availableBookies, data.clone());
        }

        @Override
        public void close() {
            ledger.seal();
        }
    }
}
