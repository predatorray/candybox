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

package me.predatorray.candybox.store.testsupport;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An OutputStream wrapper which will throw IOException at a given offset while being written.
 *
 * <p>This class may be useful for unit tests which cover exception handling while writing to an OutputStream</p>
 *
 * @author Wenhao Ji
 */
public class ExceptionalOutputStream extends OutputStream {

    private final OutputStream out;
    private final int brokenOffset;
    private final IOException exceptionThrownOnOffset;

    private int offset = 0;

    public ExceptionalOutputStream(OutputStream out, int brokenOffset, IOException exceptionThrownOnOffset) {
        this.out = out;
        this.brokenOffset = brokenOffset;
        this.exceptionThrownOnOffset = exceptionThrownOnOffset;
    }

    @Override
    public void write(int b) throws IOException {
        incrementAndThrowIfBroken(1);
        out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        incrementAndThrowIfBroken(b.length);
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        incrementAndThrowIfBroken(len);
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    private void incrementAndThrowIfBroken(int bytes) throws IOException {
        offset += bytes;
        if (offset >= brokenOffset) {
            throw exceptionThrownOnOffset;
        }
    }
}
