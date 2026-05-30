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
package me.predatorray.candybox.lsm.engine;

import java.util.List;
import me.predatorray.candybox.common.CandyKey;

/**
 * A page of {@code listCandies} results over the merged, tombstone-suppressed view.
 *
 * @param entries        the keys on this page, ascending
 * @param nextStartAfter continuation cursor (pass as {@code startAfter} to resume), or {@code null}
 *                       when the listing is exhausted
 */
public record ListResult(List<ListEntry> entries, String nextStartAfter) {

    public ListResult {
        entries = List.copyOf(entries);
    }

    public boolean isTruncated() {
        return nextStartAfter != null;
    }

    /**
     * One listing row.
     *
     * @param key             the CandyKey
     * @param contentLength   length in bytes
     * @param createdAtMillis creation time
     */
    public record ListEntry(CandyKey key, long contentLength, long createdAtMillis) {
    }
}
