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

import me.predatorray.candybox.util.Validations;

import java.io.IOException;
import java.io.InputStream;

public class InputStreamWrapper<T extends InputStream> extends InputStream {

    private final T wrapped;

    public InputStreamWrapper(T wrapped) {
        this.wrapped = Validations.notNull(wrapped);
    }

    @Override
    public int read() throws IOException {
        return wrapped.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return wrapped.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return wrapped.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return wrapped.skip(n);
    }

    @Override
    public int available() throws IOException {
        return wrapped.available();
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }

    @Override
    public void mark(int readlimit) {
        wrapped.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        wrapped.reset();
    }

    @Override
    public boolean markSupported() {
        return wrapped.markSupported();
    }

    protected T getInputStream() {
        return wrapped;
    }
}
