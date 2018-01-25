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

import org.junit.Assert;
import org.junit.Test;

public class BlockLocationTest {

    @Test(expected = IllegalArgumentException.class)
    public void offsetMustNotBeNegative() {
        new BlockLocation(-1L, 1L);
    }

    @Test
    public void zeroOffsetIsAllowed() {
        long offset = 0L;
        long length = 1L;
        BlockLocation blockLocation = new BlockLocation(offset, length);
        Assert.assertEquals(offset, blockLocation.getOffset());
        Assert.assertEquals(length, blockLocation.getLength());
    }

    @Test(expected = IllegalArgumentException.class)
    public void lengthMustNotBeZero() {
        new BlockLocation(1L, 0L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void lengthMustNotBeNegative() {
        new BlockLocation(1L, -1L);
    }

    @Test
    public void withinSuperBlockRange() {
        long offset = 10L;
        long length = 10L;
        long blockSize = offset + length;
        Assert.assertFalse(new BlockLocation(offset, length).isOutOfRange(blockSize));
    }

    @Test
    public void outOfSuperBlockRange() {
        long offset = 10L;
        long length = 10L;
        long blockSize = offset + length + 1;
        Assert.assertFalse(new BlockLocation(offset, length).isOutOfRange(blockSize));
    }

    @Test
    public void blockLocationsWithSameOffsetAndLengthAreEqual() {
        long offset = 1L;
        long length = 2L;
        BlockLocation a = new BlockLocation(offset, length);
        BlockLocation b = new BlockLocation(offset, length);
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }
}