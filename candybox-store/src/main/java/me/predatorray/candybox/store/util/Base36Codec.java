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

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class Base36Codec implements FilenameSafeStringCodec {

    private static final int RADIX = 36;
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private final Charset charset;

    public Base36Codec() {
        this(DEFAULT_CHARSET);
    }

    public Base36Codec(Charset charset) {
        this.charset = Objects.requireNonNull(charset, "charset must not be null");
    }

    @Override
    public String encode(String original) {
        if (original == null) {
            return null;
        }
        if (original.isEmpty()) {
            return "";
        }

        byte[] binary = original.getBytes(charset);
        return new BigInteger(binary).toString(RADIX);
    }

    @Override
    public String decode(String encodedFilename) {
        if (encodedFilename == null) {
            return null;
        }
        if (encodedFilename.isEmpty()) {
            return "";
        }
        byte[] binary = new BigInteger(encodedFilename, RADIX).toByteArray();
        return new String(binary, charset);
    }
}
