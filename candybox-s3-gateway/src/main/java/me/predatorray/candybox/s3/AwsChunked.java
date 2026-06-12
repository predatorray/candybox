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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Decodes the AWS {@code aws-chunked} content-encoding framing from a request body, recovering the
 * raw object bytes plus the per-chunk signatures and trailing headers — so the caller
 * ({@link S3Authenticator#verifiedBody}) can verify the {@code STREAMING-AWS4-HMAC-SHA256-PAYLOAD}
 * signature chain and the {@code STREAMING-*-TRAILER} checksums the modern SDKs send by default.
 *
 * <pre>
 *   &lt;hex-size&gt;[;chunk-signature=...]\r\n &lt;size bytes&gt; \r\n
 *   ...
 *   0[;chunk-signature=...]\r\n [trailer-name: value\r\n ...] \r\n
 * </pre>
 *
 * <p>If we stored the framing verbatim the object would be corrupted, so this decoder must run
 * before the bytes reach {@code putCandy}. See {@code S3_GATEWAY_PLAN.md} §10.
 */
final class AwsChunked {

    private AwsChunked() {
    }

    /** One decoded chunk: its raw bytes and the {@code chunk-signature} (null when unsigned). */
    record Chunk(byte[] data, String signature) {
    }

    /** The decoded body: payload bytes, the chunk list (incl. the empty final chunk when signed),
     * and any trailing headers (lower-cased names). */
    record Decoded(byte[] payload, List<Chunk> chunks, Map<String, String> trailers) {
    }

    /** Whether a request body is {@code aws-chunked} framed, from its headers. */
    static boolean isChunked(String contentEncoding, String contentSha256) {
        if (contentEncoding != null && contentEncoding.toLowerCase().contains("aws-chunked")) {
            return true;
        }
        return contentSha256 != null && contentSha256.startsWith("STREAMING-");
    }

    /** Decodes the framed body into the raw payload + signatures + trailers. */
    static Decoded decode(byte[] framed) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, framed.length));
        List<Chunk> chunks = new ArrayList<>();
        Map<String, String> trailers = new LinkedHashMap<>();
        int pos = 0;
        while (pos < framed.length) {
            int eol = indexOfCrlf(framed, pos);
            if (eol < 0) {
                throw new S3Exception(S3ErrorCode.INVALID_REQUEST,
                        "Malformed aws-chunked body: no chunk header");
            }
            String header = new String(framed, pos, eol - pos, StandardCharsets.US_ASCII);
            pos = eol + 2;
            int semi = header.indexOf(';');
            String sizeHex = (semi >= 0 ? header.substring(0, semi) : header).trim();
            String signature = null;
            if (semi >= 0) {
                String ext = header.substring(semi + 1).trim();
                if (ext.startsWith("chunk-signature=")) {
                    signature = ext.substring("chunk-signature=".length()).trim();
                }
            }
            int size;
            try {
                size = Integer.parseInt(sizeHex, 16);
            } catch (NumberFormatException e) {
                throw new S3Exception(S3ErrorCode.INVALID_REQUEST,
                        "Malformed aws-chunked size: " + sizeHex);
            }
            if (size == 0) {
                chunks.add(new Chunk(new byte[0], signature));
                pos = readTrailers(framed, pos, trailers);
                break;
            }
            if (pos + size > framed.length) {
                throw new S3Exception(S3ErrorCode.INVALID_REQUEST, "Truncated aws-chunked chunk");
            }
            byte[] data = new byte[size];
            System.arraycopy(framed, pos, data, 0, size);
            chunks.add(new Chunk(data, signature));
            out.write(framed, pos, size);
            pos += size;
            // Each chunk's data is followed by a CRLF.
            if (pos + 2 <= framed.length && framed[pos] == '\r' && framed[pos + 1] == '\n') {
                pos += 2;
            }
        }
        return new Decoded(out.toByteArray(), chunks, trailers);
    }

    /** Parses {@code name: value\r\n} trailer lines after the final chunk, until a blank line/EOF. */
    private static int readTrailers(byte[] framed, int pos, Map<String, String> trailers) {
        while (pos < framed.length) {
            int eol = indexOfCrlf(framed, pos);
            if (eol < 0) {
                // Tolerate a final unterminated trailer line.
                eol = framed.length;
            }
            if (eol == pos) {
                return eol + 2; // blank line ends the trailers
            }
            String line = new String(framed, pos, eol - pos, StandardCharsets.US_ASCII);
            int colon = line.indexOf(':');
            if (colon > 0) {
                trailers.put(line.substring(0, colon).trim().toLowerCase(Locale.ROOT),
                        line.substring(colon + 1).trim());
            }
            pos = eol + 2;
        }
        return pos;
    }

    private static int indexOfCrlf(byte[] b, int from) {
        for (int i = from; i + 1 < b.length; i++) {
            if (b[i] == '\r' && b[i + 1] == '\n') {
                return i;
            }
        }
        return -1;
    }
}
