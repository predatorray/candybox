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
