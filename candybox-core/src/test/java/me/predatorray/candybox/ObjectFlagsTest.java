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

package me.predatorray.candybox;

import org.junit.Assert;
import org.junit.Test;

public class ObjectFlagsTest {

    @Test
    public void testFlagDeleted() {
        Assert.assertTrue(ObjectFlags.isDeleted(ObjectFlags.DELETED));
    }

    @Test
    public void testCompoundFlagDeleted() {
        short flag = ObjectFlags.DELETED | (1 << 1);
        Assert.assertTrue(ObjectFlags.isDeleted(flag));
    }

    @Test
    public void testFlagNotDeleted() {
        Assert.assertFalse(ObjectFlags.isDeleted(ObjectFlags.NONE));
    }
}
