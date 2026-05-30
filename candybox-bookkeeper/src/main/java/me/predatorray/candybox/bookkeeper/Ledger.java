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

import java.util.Map;

/**
 * A handle on a ledger, shared by the readable and writable views. A ledger has at most one writer in
 * its lifetime and becomes immutable ("sealed") once closed or recovered.
 *
 * <p>Implementations must be safe for use by a single owning thread of control; concurrent reads on a
 * {@link ReadableLedger} are permitted.
 */
public interface Ledger extends AutoCloseable {

    /** The unique ledger id. */
    long ledgerId();

    /**
     * The last-add-confirmed entry id: the highest entry id durably acknowledged. {@code -1} for an
     * empty ledger. After a {@code recover-open} this is the deterministic recovered tail.
     */
    long lastAddConfirmed();

    /** Whether the ledger is sealed (closed or recovered) and can no longer accept appends. */
    boolean isSealed();

    /** Immutable custom metadata stamped on the ledger at creation. */
    Map<String, byte[]> customMetadata();

    /** Closes this handle, sealing the ledger if this handle is the writer. Idempotent. */
    @Override
    void close();
}
