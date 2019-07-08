/*
 * Copyright (c) 2018 the original author or authors.
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

package me.predatorray.candybox.store.util;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class ByteBufferInputStream extends InputStream {

    private final Iterator<? extends ByteBuffer> byteBufferIterator;

    private ByteBuffer current = null;

    public ByteBufferInputStream(Iterable<? extends ByteBuffer> byteBuffers) {
        this.byteBufferIterator = byteBuffers.iterator();
    }

    private ByteBuffer nextAvailableByteBuffer() {
        if (current != null && current.hasRemaining()) {
            return current;
        }
        while (byteBufferIterator.hasNext()) {
            current = byteBufferIterator.next();
            if (current.hasRemaining()) {
                return current;
            }
        }
        return null;
    }

    @Override
    public int read() {
        ByteBuffer next = nextAvailableByteBuffer();
        if (next == null) {
            return -1;
        }
        return next.get() & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (b.length == 0 || len == 0) {
            return 0;
        }
        ByteBuffer next = nextAvailableByteBuffer();
        if (next == null) {
            return -1;
        }

        int remaining = next.remaining();
        int maxBytesRead = Math.min(b.length, remaining);
        next.get(b, off, maxBytesRead);
        return maxBytesRead;
    }

    @Override
    public long skip(long n) {
        if (n <= 0) {
            return 0;
        }

        long skipped = n;
        while (true) {
            ByteBuffer next = nextAvailableByteBuffer();
            if (next == null) {
                break;
            }
            int remaining = next.remaining();
            if ((n - skipped) <= remaining) {
                next.position(next.position() + (int) (n - skipped));
                break;
            }
            next.position(next.limit());
            skipped += remaining;
        }
        return n - skipped;
    }

    @Override
    public int available() {
        ByteBuffer next = nextAvailableByteBuffer();
        if (next == null) {
            return 0;
        }
        return next.remaining();
    }

    @Override
    public void close() {}
}
