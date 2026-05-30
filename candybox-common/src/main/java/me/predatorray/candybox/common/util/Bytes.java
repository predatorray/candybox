/*
 * Copyright (c) 2026 the original author or authors.
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
package me.predatorray.candybox.common.util;

import java.util.Comparator;

/** Byte-array helpers shared across modules, notably unsigned lexicographic comparison. */
public final class Bytes {

    /** Unsigned lexicographic order — the canonical ordering for keys in SSTables and merges. */
    public static final Comparator<byte[]> LEXICOGRAPHIC = Bytes::compare;

    private Bytes() {
    }

    /**
     * The smallest byte array strictly greater than every array beginning with {@code prefix} — i.e.
     * the exclusive upper bound of the prefix range. Returns {@code null} when no such bound exists
     * (an empty prefix, or one consisting only of {@code 0xFF} bytes), meaning "unbounded above".
     *
     * <p>Computed by incrementing the last byte that is below {@code 0xFF} and dropping the trailing
     * {@code 0xFF} run, matching the unsigned ordering used everywhere for keys.
     */
    public static byte[] prefixSuccessor(byte[] prefix) {
        int i = prefix.length - 1;
        while (i >= 0 && (prefix[i] & 0xFF) == 0xFF) {
            i--;
        }
        if (i < 0) {
            return null; // empty, or all 0xFF: nothing sorts above the whole prefix range
        }
        byte[] out = new byte[i + 1];
        System.arraycopy(prefix, 0, out, 0, i + 1);
        out[i] = (byte) ((out[i] & 0xFF) + 1);
        return out;
    }

    /** Compares two arrays byte-by-byte treating each byte as unsigned (0..255). */
    public static int compare(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return a.length - b.length;
    }
}
