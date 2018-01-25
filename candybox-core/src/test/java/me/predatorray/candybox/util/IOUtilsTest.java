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

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class IOUtilsTest {

    @Test
    public void nullCloseableIsSkipped() throws Exception {
        IOUtils.closeAndSuppress(null, null);
    }

    @Test
    public void resourceIsClosed() throws Exception {
        Closeable closeable = mock(Closeable.class);
        IOUtils.closeAndSuppress(closeable, null);

        verify(closeable).close();
    }

    @Test
    public void resourceIsClosedAndThrowableIsThrown() throws Exception {
        Exception exceptionThrown = new Exception();
        Closeable closeable = mock(Closeable.class);
        try {
            IOUtils.closeAndSuppress(closeable, exceptionThrown);
            Assert.fail("exception should be thrown: " + exceptionThrown);
        } catch (Exception e) {
            Assert.assertSame(exceptionThrown, e);
        }
    }

    @Test
    public void resourceIsClosedAndThrowableIsSuppressed() throws Exception {
        IOException exceptionSuppressed = new IOException();
        IOException exceptionThrown = new IOException();
        Closeable closeable = mock(Closeable.class);
        doThrow(exceptionSuppressed).when(closeable).close();

        try {
            IOUtils.closeAndSuppress(closeable, exceptionThrown);
            Assert.fail("exception should be thrown: " + exceptionThrown);
        } catch (Exception e) {
            Assert.assertSame(exceptionThrown, e);
            Assert.assertEquals(Collections.singletonList(exceptionSuppressed),
                    Arrays.asList(exceptionThrown.getSuppressed()));
        }
    }
}
