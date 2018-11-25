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

package me.predatorray.candybox.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class IOUtilsAddSuppressIfThrownTest<T extends Throwable> {

    private static final Exception EX1 = new Exception();

    private static final Exception EX2 = new Exception();

    private static final IOException IO_EX1 = new IOException();

    static {
        EX2.addSuppressed(EX1);
    }

    private final T nontrivial;
    private final Closeable closeable;
    private final IOException expectedSuppressed;
    private final int expectedSuppressedOffset;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {EX1, (Closeable) () -> {}, null, 0},
                {EX1, (Closeable) () -> {throw IO_EX1;}, IO_EX1, 0},
                {EX2, (Closeable) () -> {throw IO_EX1;}, IO_EX1, 1}
        });
    }

    public IOUtilsAddSuppressIfThrownTest(T nontrivial, Closeable closeable,
                                          IOException expectedSuppressed, int expectedSuppressedOffset) {
        this.nontrivial = nontrivial;
        this.closeable = closeable;
        this.expectedSuppressed = expectedSuppressed;
        this.expectedSuppressedOffset = expectedSuppressedOffset;
    }

    @Test
    public void exercise() {
        T t = IOUtils.addSuppressIfThrown(nontrivial, closeable);
        assertSame(nontrivial, t);

        if (expectedSuppressed == null) {
            return;
        }
        Throwable[] suppressed = nontrivial.getSuppressed();
        assertTrue(expectedSuppressedOffset < suppressed.length);
        assertEquals(expectedSuppressed, suppressed[expectedSuppressedOffset]);
    }
}