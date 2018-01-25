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

public class EncodingUtils {

    public static long toUnsignedInt(int n, boolean isZeroMaximum) {
        if (n == 0 && isZeroMaximum) {
            return 1L << 32;
        }
        return n & 0xffffffffL;
    }

    public static int toUnsignedShort(short n, boolean isZeroMaximum) {
        if (n == 0 && isZeroMaximum) {
            return 1 << 16;
        } else {
            return n;
        }
    }
}
