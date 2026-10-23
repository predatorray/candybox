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
package me.predatorray.candybox.lsm.iterator;

import java.util.Iterator;
import java.util.List;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Mutation;

/** A single-direction mutation iterator that can skip ahead without being re-opened. */
public interface SeekableIterator extends Iterator<Mutation> {

    /**
     * Advances to the first entry at or past {@code target} in the scan direction (ascending: key
     * {@code >= target}; descending: key {@code <= target}). Never moves backwards.
     */
    void seek(CandyKey target);

    /** Wraps an iterator that has nothing left to return. */
    static SeekableIterator empty() {
        Iterator<Mutation> none = List.<Mutation>of().iterator();
        return new SeekableIterator() {
            @Override
            public boolean hasNext() {
                return none.hasNext();
            }

            @Override
            public Mutation next() {
                return none.next();
            }

            @Override
            public void seek(CandyKey target) {
            }
        };
    }
}
