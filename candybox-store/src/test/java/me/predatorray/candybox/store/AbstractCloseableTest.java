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

package me.predatorray.candybox.store;

import org.junit.Test;

import static org.junit.Assert.*;

public class AbstractCloseableTest {

    public static class TestedAbstractCloseable extends AbstractCloseable {
    }

    @Test
    public void testIfNotClosed() throws Exception {
        TestedAbstractCloseable sut = new TestedAbstractCloseable();
        assertFalse(sut.isClosed());
        sut.ensureNotClosed(); // throw no exception
    }

    @Test
    public void closeCalledMultipleTimes() throws Exception {
        TestedAbstractCloseable sut = new TestedAbstractCloseable();
        sut.close();
        sut.close();
        assertTrue(sut.isClosed());
    }

    @Test(expected = AlreadyClosedException.class)
    public void testIfClosed() throws Exception {
        TestedAbstractCloseable sut = new TestedAbstractCloseable();
        sut.close();

        assertTrue(sut.isClosed());

        sut.ensureNotClosed();
    }
}
