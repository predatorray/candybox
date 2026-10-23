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
import java.util.function.Function;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Mutation;

/**
 * Makes a cheaply re-openable source (e.g. a memtable view) seekable by re-opening it at the target.
 * {@code open} receives the start key, inclusive in the scan direction, or {@code null} for the start.
 */
public final class ReopeningIterator implements SeekableIterator {

    private final Function<CandyKey, Iterator<Mutation>> open;
    private Iterator<Mutation> current;

    public ReopeningIterator(Function<CandyKey, Iterator<Mutation>> open, CandyKey start) {
        this.open = open;
        this.current = open.apply(start);
    }

    @Override
    public void seek(CandyKey target) {
        current = open.apply(target);
    }

    @Override
    public boolean hasNext() {
        return current.hasNext();
    }

    @Override
    public Mutation next() {
        return current.next();
    }
}
