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

/**
 * Strips the AWS {@code aws-chunked} content-encoding framing from a request body, recovering the raw
 * object bytes.
 *
 * <p>Even with no signature verification, the AWS SDKs/CLI may upload with
 * {@code Content-Encoding: aws-chunked} (signalled by an {@code x-amz-content-sha256} of
 * {@code STREAMING-*}). The wire body is then a sequence of chunks:
 *
 * <pre>
 *   &lt;hex-size&gt;[;chunk-signature=...]\r\n &lt;size bytes&gt; \r\n
 *   ...
 *   0[;chunk-signature=...]\r\n [trailers] \r\n
 * </pre>
 *
 * <p>If we stored the framing verbatim the object would be corrupted, so this decoder must run before
 * the bytes reach {@code putCandy}. The chunk signatures and trailers are ignored (we don't verify).
 * See {@code S3_GATEWAY_PLAN.md} §10.
 */
final class AwsChunked {

    private AwsChunked() {
    }

    /** Whether a request body is {@code aws-chunked} framed, from its headers. */
    static boolean isChunked(String contentEncoding, String contentSha256) {
        if (contentEncoding != null && contentEncoding.toLowerCase().contains("aws-chunked")) {
            return true;
        }
        return contentSha256 != null && contentSha256.startsWith("STREAMING-");
    }

    /** Decodes the framed body into the raw payload. {@code expectedLength} (&lt;0 if unknown) sizes the buffer. */
    static byte[] decode(byte[] framed, long expectedLength) {
        ByteArrayOutputStream out =
                new ByteArrayOutputStream(expectedLength > 0 ? (int) Math.min(expectedLength, framed.length) : 64);
        int pos = 0;
        while (pos < framed.length) {
            int eol = indexOfCrlf(framed, pos);
            if (eol < 0) {
                throw new S3Exception(S3ErrorCode.INVALID_REQUEST, "Malformed aws-chunked body: no chunk header");
            }
            String header = new String(framed, pos, eol - pos, StandardCharsets.US_ASCII);
            pos = eol + 2;
            int semi = header.indexOf(';');
            String sizeHex = (semi >= 0 ? header.substring(0, semi) : header).trim();
            int size;
            try {
                size = Integer.parseInt(sizeHex, 16);
            } catch (NumberFormatException e) {
                throw new S3Exception(S3ErrorCode.INVALID_REQUEST, "Malformed aws-chunked size: " + sizeHex);
            }
            if (size == 0) {
                break; // final chunk; trailers (if any) ignored.
            }
            if (pos + size > framed.length) {
                throw new S3Exception(S3ErrorCode.INVALID_REQUEST, "Truncated aws-chunked chunk");
            }
            out.write(framed, pos, size);
            pos += size;
            // Each chunk's data is followed by a CRLF.
            if (pos + 2 <= framed.length && framed[pos] == '\r' && framed[pos + 1] == '\n') {
                pos += 2;
            }
        }
        return out.toByteArray();
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
