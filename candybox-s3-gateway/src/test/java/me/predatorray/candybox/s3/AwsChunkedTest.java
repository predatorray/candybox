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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class AwsChunkedTest {

    @Test
    void detection() {
        assertThat(AwsChunked.isChunked("aws-chunked", null)).isTrue();
        assertThat(AwsChunked.isChunked(null, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")).isTrue();
        assertThat(AwsChunked.isChunked(null, "UNSIGNED-PAYLOAD")).isFalse();
        assertThat(AwsChunked.isChunked(null, null)).isFalse();
    }

    @Test
    void decodesSignedChunks() {
        String framed = "5;chunk-signature=abc123\r\nhello\r\n0;chunk-signature=def456\r\n\r\n";
        AwsChunked.Decoded out = AwsChunked.decode(framed.getBytes(StandardCharsets.UTF_8));
        assertThat(new String(out.payload(), StandardCharsets.UTF_8)).isEqualTo("hello");
        // The signatures are surfaced for the signed-streaming verification chain.
        assertThat(out.chunks()).hasSize(2);
        assertThat(out.chunks().get(0).signature()).isEqualTo("abc123");
        assertThat(out.chunks().get(1).signature()).isEqualTo("def456");
    }

    @Test
    void decodesUnsignedChunksAcrossMultipleChunks() {
        String framed = "3\r\nfoo\r\n3\r\nbar\r\n0\r\n\r\n";
        byte[] out = AwsChunked.decode(framed.getBytes(StandardCharsets.UTF_8)).payload();
        assertThat(new String(out, StandardCharsets.UTF_8)).isEqualTo("foobar");
    }

    @Test
    void terminatorOnlyBodyDecodesToEmpty() {
        byte[] out = AwsChunked.decode("0\r\n\r\n".getBytes(StandardCharsets.UTF_8)).payload();
        assertThat(out).isEmpty();
        assertThat(AwsChunked.decode(new byte[0]).payload()).isEmpty();
    }

    @Test
    void toleratesMissingCrlfBeforeTerminator() {
        // Data chunk not followed by the optional CRLF, then the 0-terminator.
        byte[] out = AwsChunked.decode("3\r\nfoo0\r\n\r\n".getBytes(StandardCharsets.UTF_8)).payload();
        assertThat(new String(out, StandardCharsets.UTF_8)).isEqualTo("foo");
    }

    @Test
    void rejectsTruncatedChunk() {
        String framed = "20\r\nonly-a-few\r\n"; // declares 32 bytes, far fewer present
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> AwsChunked.decode(framed.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(S3Exception.class);
    }
}
