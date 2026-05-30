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

import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.StorageException;

/**
 * The single-writer view over a ledger. The writer may also read back entries it has added.
 *
 * <p>Once another actor recovers (fences) this ledger, {@link #append(byte[])} fails with
 * {@link FencedException} — the property the whole zombie-owner/zombie-compactor defense rests on.
 */
public interface WritableLedger extends ReadableLedger {

    /**
     * Appends an entry, blocking until it is acknowledged by the configured ack-quorum.
     *
     * @param data the entry payload
     * @return the assigned entry id
     * @throws FencedException if the ledger has been fenced/sealed by a recovery
     * @throws StorageException if ack-quorum could not be met or the append otherwise failed
     */
    long append(byte[] data);
}
