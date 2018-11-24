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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class IOUtilsReadShortOrNoneTest {

    private final byte[] streamInput;
    private final Short expected;
    private final Class<? extends Exception> expectedExceptionClz;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {new byte[0], null, null},
                {new byte[] {1}, null, EOFException.class},
                {new byte[] {1, 1}, (short) 257, null}
        });
    }

    public IOUtilsReadShortOrNoneTest(byte[] streamInput, Short expected,
                                      Class<? extends Exception> expectedExceptionClz) {
        this.streamInput = streamInput;
        this.expected = expected;
        this.expectedExceptionClz = expectedExceptionClz;
    }

    @Test
    public void exercise() throws Exception {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(streamInput))) {
            Short actual = IOUtils.readShortOrNone(dis);
            assertEquals(expected, actual);

            if (expectedExceptionClz != null) {
                fail("An exception of class " + expectedExceptionClz.getName() + " is expected to be thrown.");
            }
        } catch (Exception ex) {
            if (expectedExceptionClz == null) {
                ex.printStackTrace();
                fail("An unexpected exception of class " + ex.getClass() + " is thrown.");
            } else {
                assertTrue(expectedExceptionClz.isAssignableFrom(ex.getClass()));
            }
        }
    }
}