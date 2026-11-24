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
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AwsChunkedFramingTest {
    @ParameterizedTest
    @ValueSource(strings = {"1", "not-hex\r\nx\r\n0\r\n\r\n"})
    void malformedChunkHeader(String framed) {
        S3Exception failure = catchThrowableOfType(() -> AwsChunked.decode(bytes(framed)), S3Exception.class);
        assertThat(failure.error().httpStatus()).isEqualTo(400);
        assertThat(failure.error().code()).isEqualTo("InvalidRequest");
        assertThat(failure).hasMessageContaining(framed.equals("1") ? "no chunk header" : "Malformed aws-chunked size");
    }

    @Test
    void trailersIncludeMultipleLinesAndUnterminatedLastLineAndIgnoreMissingColon() {
        AwsChunked.Decoded decoded = AwsChunked.decode(bytes(
                "1\r\nx\r\n0\r\nX-First: one\r\nno-colon\r\nX-Second: two:three\r\n X-Last : final "));
        assertThat(decoded.payload()).isEqualTo(bytes("x"));
        assertThat(decoded.trailers()).containsExactly(
                java.util.Map.entry("x-first", "one"), java.util.Map.entry("x-second", "two:three"),
                java.util.Map.entry("x-last", "final"));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
