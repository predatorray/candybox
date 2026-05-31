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
package me.predatorray.candybox.s3;

/**
 * Derives the S3 {@code ETag} header from the CRC32C that Candybox already stores for every object.
 *
 * <p>The value is the unsigned 32-bit CRC32C left-padded to 32 hex chars (so it has the visual shape
 * of an MD5 ETag), e.g. {@code crc32c=0x8f3a2b1c -> "000000008f3a2b1c"}. It is deterministic and stable
 * across PUT/GET/HEAD/List for the same bytes, and needs no extra storage.
 *
 * <p><b>v1 limitation:</b> this is <em>not</em> the MD5 that real S3 returns, so a client that
 * independently computes MD5 and strictly compares may warn. Real MD5 ETags are a deferred feature
 * (they require a new object-metadata layer to persist the digest); when that lands, only this class
 * changes. See {@code S3_GATEWAY_PLAN.md} §8, §16.
 */
final class Etag {

    private Etag() {
    }

    /** Returns the quoted ETag string (including the surrounding double quotes) for a CRC32C value. */
    static String of(int crc32c) {
        return '"' + unquoted(crc32c) + '"';
    }

    /** Returns the bare ETag hex (no surrounding quotes), e.g. for XML list entries. */
    static String unquoted(int crc32c) {
        return String.format("%032x", crc32c & 0xffffffffL);
    }

    /**
     * Parses a quoted or unquoted ETag that this class produced back to the CRC32C int. Lenient about
     * a possible {@code -N} multipart suffix (which is dropped — the CRC is the part of interest).
     *
     * @throws S3Exception {@link S3ErrorCode#INVALID_ARGUMENT} if the ETag is not in the format
     *                     Candybox produces
     */
    static int parseCrc32cHex(String etag) {
        if (etag == null) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Missing ETag");
        }
        String s = etag.trim();
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
            s = s.substring(1, s.length() - 1);
        }
        int dash = s.indexOf('-');
        if (dash >= 0) {
            s = s.substring(0, dash);
        }
        if (s.length() < 8) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Malformed ETag: " + etag);
        }
        // Take the low 8 hex chars (the CRC32C). Higher chars are the zero-padding.
        String hex = s.substring(s.length() - 8);
        try {
            return (int) (Long.parseLong(hex, 16) & 0xffffffffL);
        } catch (NumberFormatException e) {
            throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT, "Malformed ETag: " + etag);
        }
    }
}
