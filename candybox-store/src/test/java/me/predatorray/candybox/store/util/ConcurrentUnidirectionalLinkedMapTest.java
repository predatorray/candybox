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

package me.predatorray.candybox.store.util;

import org.junit.Assert;
import org.junit.Test;

public class ConcurrentUnidirectionalLinkedMapTest {

    @Test
    public void valueCanBeRetrievedAfterPut() {
        final String key = "key";
        final Object value = new Object();
        ConcurrentUnidirectionalLinkedMap<String, Object> sut = new ConcurrentUnidirectionalLinkedMap<>(1);

        Assert.assertTrue(sut.put(key, value));
        Assert.assertSame(value, sut.get(key));
    }

    @Test
    public void valueIsRejectedIfFull() {
        ConcurrentUnidirectionalLinkedMap<String, Object> sut = new ConcurrentUnidirectionalLinkedMap<>(1);

        Assert.assertTrue(sut.put("key1", new Object()));
        Assert.assertFalse(sut.put("key2", new Object()));
    }
}
