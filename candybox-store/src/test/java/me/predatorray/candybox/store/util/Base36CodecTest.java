/*
 * Copyright (c) 2019 the original author or authors.
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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class Base36CodecTest {

    private Base36Codec sut;

    @Before
    public void setUpCodec() {
        sut = new Base36Codec();
    }

    @Test
    public void nullIsEncodedAsNull() {
        assertNull(sut.encode(null));
    }

    @Test
    public void nullIsDecodedAsNull() {
        assertNull(sut.decode(null));
    }

    @Test
    public void dataCanBeReconstructedAfterEncodingAndDecoding() {
        String original = "000";
        assertEquals(original, sut.decode(sut.encode(original)));
    }

    @Test
    public void emptyStringCanBeReconstructedAfterEncodingAndDecoding() {
        String original = "";
        assertEquals(original, sut.decode(sut.encode(original)));
    }
}
