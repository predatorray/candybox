/*
 * Copyright (c) 2017 the original author or authors.
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

import static org.junit.Assert.*;

public class ValidationsTest {

    @Test
    public void returnsTheSameObjectIfNotNull() throws Exception {
        Object object = new Object();
        Object validated = Validations.notNull(object);
        assertSame(object, validated);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentExceptionIfNull() throws Exception {
        Validations.notNull(null);
    }

    @Test
    public void oneIsPositive() throws Exception {
        final long l = 1L;
        Assert.assertEquals(l, Validations.positive(l));

        final int i = 1;
        Assert.assertEquals(i, Validations.positive(i));
    }

    @Test(expected = IllegalArgumentException.class)
    public void zeroIsNotPositive() throws Exception {
        Validations.positive(0L);
    }

    @Test
    public void zeroIsNonNegative() throws Exception {
        final long n = 0L;
        long validated = Validations.nonnegative(n);
        Assert.assertEquals(n, validated);
    }

    @Test(expected = IllegalArgumentException.class)
    public void minusOneIsNotNonNegative() throws Exception {
        Validations.nonnegative(-1L);
    }

    @Test
    public void returnsTheSameObjectIfConditionSatisfied() throws Exception {
        Object object = new Object();
        Object validated = Validations.that(object, true, "");
        assertSame(object, validated);
    }

    @Test
    public void exceptionIsThrownIfConditionNotSatisfied() throws Exception {
        String message = "message";
        Object object = new Object();
        try {
            Validations.that(object, false, message);
            fail("IllegalArgumentException is expected to be thrown");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(message, e.getMessage());
        }
    }

    @Test
    public void returnsTheSameObjectIfConditionFunctionSatisfied() throws Exception {
        Object object = new Object();
        Object validated = Validations.that(object, obj -> true, "");
        assertSame(object, validated);
    }

    @Test
    public void exceptionIsThrownIfConditionFunctionNotSatisfied() throws Exception {
        String message = "message";
        Object object = new Object();
        try {
            Validations.that(object, obj -> false, message);
            fail("IllegalArgumentException is expected to be thrown");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(message, e.getMessage());
        }
    }
}
