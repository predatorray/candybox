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

import java.nio.charset.StandardCharsets;

public class ObjectKeyTest {

    @Test(expected = IllegalArgumentException.class)
    public void emptyStringKeyIsNotAllowed() throws Exception {
        new ObjectKey("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyByteArrayKeyIsNotAllowed() throws Exception {
        new ObjectKey(new byte[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullStringKeyIsNotAllowed() throws Exception {
        new ObjectKey((String) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullByteArrayKeyIsNotAllowed() throws Exception {
        new ObjectKey((byte[]) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void keySizeCannotExceedTheMaximumLimit() throws Exception {
        new ObjectKey(new byte[ObjectKey.MAXIMUM_KEY_SIZE + 1]);
    }

    @Test
    public void createAnInstanceWithTheMaximumKeySize() throws Exception {
        new ObjectKey(new byte[ObjectKey.MAXIMUM_KEY_SIZE]);
    }

    @Test
    public void twoObjectKeysWithSameStringKeyNamesAreEqual() throws Exception {
        String stringKey = "foobar";
        ObjectKey a = new ObjectKey(stringKey);
        ObjectKey b = new ObjectKey(stringKey);
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void oneCanBeReconstructedFromItsStringRepresentation() throws Exception {
        String stringKey = "foobar";
        ObjectKey objectKey = new ObjectKey(stringKey);
        ObjectKey reconstructed = new ObjectKey(objectKey.getKeyAsString());
        Assert.assertEquals(objectKey, reconstructed);
    }

    @Test
    public void oneCanBeReconstructedFromItsBinary() throws Exception {
        String stringKey = "foobar";
        ObjectKey objectKey = new ObjectKey(stringKey);
        ObjectKey reconstructed = new ObjectKey(objectKey.getBinary());
        Assert.assertEquals(objectKey, reconstructed);
    }

    @Test
    public void binaryRepresentationIsStringEncodedInAscii() throws Exception {
        String stringKey = "foobar";
        Assert.assertEquals(new ObjectKey(stringKey), new ObjectKey(stringKey.getBytes(StandardCharsets.US_ASCII)));
    }

    @Test
    public void toStringReturnsTheSameAsStringRepresentation() throws Exception {
        String stringKey = "foobar";
        Assert.assertEquals(stringKey, new ObjectKey(stringKey).toString());
    }

    @Test
    public void testLengthOfObjectKey() throws Exception {
        String stringKey = "foobar";
        ObjectKey objectKey = new ObjectKey(stringKey);
        Assert.assertEquals(stringKey.length(), objectKey.getSize());
        Assert.assertEquals((short) stringKey.length(), objectKey.getSizeAsUnsignedShort());
    }
}