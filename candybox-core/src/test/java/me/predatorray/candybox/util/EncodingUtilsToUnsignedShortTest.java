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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class EncodingUtilsToUnsignedShortTest {

    private final short input;
    private final boolean isZeroMaximum;
    private final int expected;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Short.MIN_VALUE, false, (1 << 15) },
                { (short) -1, false, ((1 << 16) - 1) },
                { (short) 0, false, 0 },
                { (short) 1, false, 1 },
                { Short.MAX_VALUE, false, (int) Short.MAX_VALUE },
        });
    }

    public EncodingUtilsToUnsignedShortTest(short input, boolean isZeroMaximum, int expected) {
        this.input = input;
        this.isZeroMaximum = isZeroMaximum;
        this.expected = expected;
    }

    @Test
    public void exercise() {
        int unsignedInt = EncodingUtils.toUnsignedShort(input, isZeroMaximum);
        Assert.assertEquals(expected, unsignedInt);
    }
}
