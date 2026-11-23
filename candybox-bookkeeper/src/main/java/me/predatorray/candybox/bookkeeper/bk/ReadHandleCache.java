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
    private boolean closed;

    ReadHandleCache(int capacity, LongFunction<ReadableLedger> opener) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.opener = opener;
    }

    synchronized ReadableLedger acquire(long id) {
        if (closed) {
            throw new StorageException("Ledger store is closed");
        }
        Entry entry = entries.get(id);
        if (entry == null) {
            entry = new Entry(opener.apply(id));
            entries.put(id, entry);
        }
        entry.borrowers++;
        if (entries.size() > capacity) {
            Long eldest = entries.keySet().iterator().next();
            invalidate(eldest);
        }
        return new Borrowed(entry);
    }

    synchronized void invalidate(long id) {
        Entry entry = entries.remove(id);
        if (entry != null) {
            retire(entry);
        }
    }

    private void retire(Entry entry) {
        entry.retired = true;
        if (entry.borrowers == 0) {
            closePhysical(entry);
        }
    }

    private void closePhysical(Entry entry) {
        try {
            entry.handle.close();
        } catch (RuntimeException failure) {
            LOG.warn("Failed closing cached read handle {}", entry.handle.ledgerId(), failure);
        }
    }

    private synchronized void release(Entry entry) {
        entry.borrowers--;
        if (entry.retired && entry.borrowers == 0) {
            closePhysical(entry);
        }
    }

    private synchronized void failed(Entry entry) {
        // A retired borrow must not evict a newer replacement for the same ledger.
        if (entries.get(entry.handle.ledgerId()) == entry) {
            invalidate(entry.handle.ledgerId());
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        entries.values().forEach(this::retire);
        entries.clear();
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
