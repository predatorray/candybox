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

import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.ValidationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ErrorMapperNodeErrorTest {
    @ParameterizedTest
    @CsvSource({
        "BoxNotFoundException, missing bucket, NO_SUCH_BUCKET, 404",
        "CandyNotFoundException, missing key, NO_SUCH_KEY, 404",
        "ValidationException, InvalidRange requested, INVALID_RANGE, 416",
        "ValidationException, bad argument, INVALID_ARGUMENT, 400",
        "LimitExceededException, too big, ENTITY_TOO_LARGE, 400",
        "ProtocolError, malformed request, INVALID_REQUEST, 400"
    })
    void mapsNodeErrorType(String type, String message, S3ErrorCode expected, int status) {
        CandyboxException failure = new CandyboxException(type + ": " + message);
        S3Exception mapped = ErrorMapper.toS3(failure);
        assertThat(mapped.error()).isEqualTo(expected);
        assertThat(mapped.error().httpStatus()).isEqualTo(status);
        assertThat(mapped).hasMessage(type + ": " + message).hasCause(failure);
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid range", "INVALID RANGE", "Invalid Range requested"})
    void mapsSpacedInvalidRangeRegardlessOfCase(String message) {
        assertThat(ErrorMapper.toS3(new ValidationException(message)).error())
                .isEqualTo(S3ErrorCode.INVALID_RANGE);
    }
}
