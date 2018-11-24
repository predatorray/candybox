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

public class MagicNumberTest {

    @Test
    public void textInterpretationMustBe4BytesString() {
        new MagicNumber("abcd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void exceptionBeThrownIfNot4BytesString() {
        new MagicNumber("abc");
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullTextInterpretationIsIllegal() {
        new MagicNumber(null);
    }

    @Test
    public void oneCanBeReconstructedFromItsTextInterpretation() {
        final String textInterpretation = "CBXn";
        MagicNumber magicNumber = new MagicNumber(textInterpretation);
        MagicNumber reconstructed = new MagicNumber(magicNumber.getTextInterpretation());
        Assert.assertEquals(magicNumber, reconstructed);
    }

    @Test
    public void oneCanBeReconstructedFromItsIntegerValue() {
        MagicNumber magicNumber = new MagicNumber(1);
        MagicNumber reconstructed = new MagicNumber(magicNumber.toInteger());
        Assert.assertEquals(magicNumber, reconstructed);
    }

    @Test
    public void magicNumberWithSameTextInterpretationIsEqual() {
        String textInterpretation = "1234";
        MagicNumber magicNumber1 = new MagicNumber(textInterpretation);
        MagicNumber magicNumber2 = new MagicNumber(textInterpretation);
        Assert.assertEquals(magicNumber1, magicNumber2);
        Assert.assertEquals(magicNumber1.hashCode(), magicNumber2.hashCode());
    }
}
