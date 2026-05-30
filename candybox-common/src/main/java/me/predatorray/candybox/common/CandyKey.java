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
package me.predatorray.candybox.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import me.predatorray.candybox.common.exception.ValidationException;
import me.predatorray.candybox.common.util.Bytes;

/**
 * A within-Box key (the S3 "object key"), a non-empty UTF-8 string.
 *
 * <p>Ordering is by <em>unsigned UTF-8 byte</em> lexicographic order (not Java's UTF-16
 * {@code String} order), so it matches the on-ledger SSTable ordering and {@code listCandies} range
 * scans exactly, including for non-ASCII keys. The UTF-8 bytes are cached.
 *
 * <p>Length validation against the configured {@code maxCandyKeyBytes} happens at the client and node
 * boundaries; this type only rejects null/empty.
 */
public final class CandyKey implements Comparable<CandyKey> {

    private final String value;
    private final byte[] utf8;

    public CandyKey(String value) {
        if (value == null || value.isEmpty()) {
            throw new ValidationException("CandyKey must be a non-empty string");
        }
        this.value = value;
        this.utf8 = value.getBytes(StandardCharsets.UTF_8);
    }

    private CandyKey(byte[] utf8, boolean owned) {
        this.utf8 = owned ? utf8 : utf8.clone();
        this.value = new String(this.utf8, StandardCharsets.UTF_8);
        if (this.utf8.length == 0) {
            throw new ValidationException("CandyKey must be a non-empty string");
        }
    }

    public static CandyKey of(String value) {
        return new CandyKey(value);
    }

    /** Builds a key from raw UTF-8 bytes (e.g. when decoding an SSTable entry). */
    public static CandyKey ofUtf8(byte[] utf8) {
        return new CandyKey(utf8, false);
    }

    public String value() {
        return value;
    }

    /** The cached UTF-8 bytes. Treat as read-only. */
    public byte[] utf8Bytes() {
        return utf8;
    }

    public int byteLength() {
        return utf8.length;
    }

    @Override
    public int compareTo(CandyKey o) {
        return Bytes.compare(this.utf8, o.utf8);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof CandyKey other && Arrays.equals(utf8, other.utf8);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(utf8);
    }

    @Override
    public String toString() {
        return value;
    }
}
