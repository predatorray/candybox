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
package me.predatorray.candybox.bookkeeper.bk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.common.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounded LRU of shared physical read handles, pinned for each borrowed view's lifetime. */
final class ReadHandleCache implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ReadHandleCache.class);
    private final int capacity;
    private final LongFunction<ReadableLedger> opener;
    private final Map<Long, Entry> entries = new LinkedHashMap<>(16, .75f, true);
    private final Map<Long, Opening> openings = new HashMap<>();
    private boolean closed;

    ReadHandleCache(int capacity, LongFunction<ReadableLedger> opener) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.opener = opener;
    }

    ReadableLedger acquire(long id) {
        Opening opening;
        synchronized (this) {
            ensureOpen();
            Entry cached = entries.get(id);
            if (cached != null) {
                return borrow(cached);
            }
            opening = openings.computeIfAbsent(id, ignored -> new Opening());
            opening.users++;
        }
        try {
            // Only misses for this ledger wait for its metadata open. Warm borrows, releases,
            // invalidations and opens for other ledgers never wait behind this network I/O.
            synchronized (opening) {
                long generation;
                synchronized (this) {
                    ensureOpen();
                    Entry cached = entries.get(id);
                    if (cached != null) {
                        return borrow(cached);
                    }
                    generation = opening.generation;
                }
                ReadableLedger physical = opener.apply(id);
                Entry evicted = null;
                ReadableLedger borrowed = null;
                synchronized (this) {
                    if (!closed) {
                        Entry entry = new Entry(physical);
                        borrowed = borrow(entry);
                        if (generation != opening.generation) {
                            // Invalidation during the open must not repopulate the cache.
                            entry.retired = true;
                        } else {
                            entries.put(id, entry);
                            if (entries.size() > capacity) {
                                Long eldest = entries.keySet().iterator().next();
                                evicted = retire(entries.remove(eldest));
                            }
                        }
                    }
                }
                if (borrowed == null) {
                    closePhysical(physical);
                    throw new StorageException("Ledger store is closed");
                }
                if (evicted != null) {
                    closePhysical(evicted.handle);
                }
                return borrowed;
            }
        } finally {
            synchronized (this) {
                if (--opening.users == 0) {
                    openings.remove(id, opening);
                }
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new StorageException("Ledger store is closed");
        }
    }

    /** Called under the cache monitor; performs no backend work. */
    private ReadableLedger borrow(Entry entry) {
        entry.borrowers++;
        return new Borrowed(entry);
    }

    void invalidate(long id) {
        Entry closing;
        synchronized (this) {
            Opening opening = openings.get(id);
            if (opening != null) {
                opening.generation++;
            }
            Entry entry = entries.remove(id);
            closing = entry == null ? null : retire(entry);
        }
        if (closing != null) {
            closePhysical(closing.handle);
        }
    }

    /** Called under the cache monitor; physical cleanup happens after unlocking. */
    private Entry retire(Entry entry) {
        entry.retired = true;
        return entry.borrowers == 0 ? entry : null;
    }

    private void closePhysical(ReadableLedger handle) {
        try {
            handle.close();
        } catch (RuntimeException failure) {
            LOG.warn("Failed closing cached read handle {}", handle.ledgerId(), failure);
        }
    }

    private void release(Entry entry) {
        boolean closing;
        synchronized (this) {
            entry.borrowers--;
            closing = entry.retired && entry.borrowers == 0;
        }
        if (closing) {
            closePhysical(entry.handle);
        }
    }

    private void failed(Entry entry) {
        synchronized (this) {
            // A retired borrow must not evict a newer replacement for the same ledger.
            if (entries.get(entry.handle.ledgerId()) == entry) {
                entries.remove(entry.handle.ledgerId());
                retire(entry); // the failed caller still pins the handle until its close
            }
        }
    }

    @Override
    public void close() {
        List<Entry> closing = new ArrayList<>();
        synchronized (this) {
            closed = true;
            for (Entry entry : entries.values()) {
                if (retire(entry) != null) {
                    closing.add(entry);
                }
            }
            entries.clear();
        }
        closing.forEach(entry -> closePhysical(entry.handle));
    }

    private static final class Opening {
        int users;
        long generation;
    }

    private static final class Entry {
        final ReadableLedger handle;
        int borrowers;
        boolean retired;

        Entry(ReadableLedger handle) {
            this.handle = handle;
        }
    }

    private final class Borrowed implements ReadableLedger {
        private final Entry entry;
        private boolean released;

        Borrowed(Entry entry) {
            this.entry = entry;
        }

        @Override
        public long ledgerId() {
            return entry.handle.ledgerId();
        }

        @Override
        public long lastAddConfirmed() {
            try {
                return entry.handle.lastAddConfirmed();
            } catch (RuntimeException failure) {
                failed(entry);
                throw failure;
            }
        }

        @Override
        public boolean isSealed() {
            return entry.handle.isSealed();
        }

        @Override
        public Map<String, byte[]> customMetadata() {
            return entry.handle.customMetadata();
        }

        @Override
        public LedgerEntry read(long entryId) {
            return readRange(entryId, entryId).get(0);
        }

        @Override
        public List<LedgerEntry> readRange(long first, long last) {
            try {
                return entry.handle.readRange(first, last);
            } catch (RuntimeException failure) {
                failed(entry);
                throw failure;
            }
        }

        @Override
        public synchronized void close() {
            if (!released) {
                released = true;
                release(entry);
            }
        }
    }
}
