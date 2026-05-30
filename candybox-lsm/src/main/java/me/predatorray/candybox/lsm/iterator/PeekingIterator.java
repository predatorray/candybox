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
import java.util.NoSuchElementException;

/** An {@link Iterator} wrapper that allows looking at the head element without consuming it. */
public final class PeekingIterator<T> implements Iterator<T> {

    private final Iterator<T> delegate;
    private boolean hasPeeked;
    private T peeked;

    public PeekingIterator(Iterator<T> delegate) {
        this.delegate = delegate;
    }

    /** Returns the next element without consuming it. */
    public T peek() {
        if (!hasPeeked) {
            peeked = delegate.next();
            hasPeeked = true;
        }
        return peeked;
    }

    @Override
    public boolean hasNext() {
        return hasPeeked || delegate.hasNext();
    }

    @Override
    public T next() {
        if (!hasPeeked) {
            return delegate.next();
        }
        T result = peeked;
        hasPeeked = false;
        peeked = null;
        return result;
    }
}
