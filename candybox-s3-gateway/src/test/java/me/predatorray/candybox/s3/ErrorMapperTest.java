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

import me.predatorray.candybox.common.exception.BoxAlreadyExistsException;
import me.predatorray.candybox.common.exception.BoxNotEmptyException;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.LimitExceededException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.common.exception.ValidationException;
import org.junit.jupiter.api.Test;

class ErrorMapperTest {

    @Test
    void concreteTypes() {
        assertThat(ErrorMapper.toS3(new CandyNotFoundException("b", "k")).error())
                .isEqualTo(S3ErrorCode.NO_SUCH_KEY);
        assertThat(ErrorMapper.toS3(new BoxAlreadyExistsException("b")).error())
                .isEqualTo(S3ErrorCode.BUCKET_ALREADY_OWNED_BY_YOU);
        assertThat(ErrorMapper.toS3(new BusyException("busy")).error())
                .isEqualTo(S3ErrorCode.SLOW_DOWN);
        assertThat(ErrorMapper.toS3(new NotOwnerException("b")).error())
                .isEqualTo(S3ErrorCode.SERVICE_UNAVAILABLE);
        assertThat(ErrorMapper.toS3(new FencedException("fenced")).error())
                .isEqualTo(S3ErrorCode.SERVICE_UNAVAILABLE);
        assertThat(ErrorMapper.toS3(new BoxNotFoundException("b")).error())
                .isEqualTo(S3ErrorCode.NO_SUCH_BUCKET);
        assertThat(ErrorMapper.toS3(new BoxNotEmptyException("b")).error())
                .isEqualTo(S3ErrorCode.BUCKET_NOT_EMPTY);
    }

    @Test
    void serviceUnavailableIsRetryable() {
        assertThat(ErrorMapper.toS3(new NotOwnerException("b")).error().retryable()).isTrue();
        assertThat(ErrorMapper.toS3(new BusyException("x")).error().retryable()).isTrue();
        assertThat(ErrorMapper.toS3(new CandyNotFoundException("b", "k")).error().retryable()).isFalse();
    }

    @Test
    void validationDistinguishesBucketName() {
        assertThat(ErrorMapper.toS3(new ValidationException("Box name must match ...")).error())
                .isEqualTo(S3ErrorCode.INVALID_BUCKET_NAME);
        assertThat(ErrorMapper.toS3(new ValidationException("something else")).error())
                .isEqualTo(S3ErrorCode.INVALID_ARGUMENT);
    }

    @Test
    void limitExceededDistinguishesKeyVsSize() {
        assertThat(ErrorMapper.toS3(new LimitExceededException("CandyKey too long")).error())
                .isEqualTo(S3ErrorCode.KEY_TOO_LONG);
        assertThat(ErrorMapper.toS3(new LimitExceededException("Candy size exceeds limit")).error())
                .isEqualTo(S3ErrorCode.ENTITY_TOO_LARGE);
    }

    @Test
    void genericClientWrappedMessages() {
        // The client collapses server errors into a generic CandyboxException prefixed with the type.
        assertThat(ErrorMapper.toS3(new CandyboxException("BoxNotEmptyException: not empty")).error())
                .isEqualTo(S3ErrorCode.BUCKET_NOT_EMPTY);
        assertThat(ErrorMapper.toS3(new CandyboxException("BoxAlreadyExistsException: exists")).error())
                .isEqualTo(S3ErrorCode.BUCKET_ALREADY_OWNED_BY_YOU);
        assertThat(ErrorMapper.toS3(new CandyboxException("Not found")).error())
                .isEqualTo(S3ErrorCode.NO_SUCH_KEY);
        assertThat(ErrorMapper.toS3(new CandyboxException("UnsupportedOperation: nope")).error())
                .isEqualTo(S3ErrorCode.NOT_IMPLEMENTED);
    }

    @Test
    void unknownFallsBackToInternalError() {
        assertThat(ErrorMapper.toS3(new IllegalStateException("boom")).error())
                .isEqualTo(S3ErrorCode.INTERNAL_ERROR);
        assertThat(ErrorMapper.toS3(new CandyboxException("WeirdThing: huh")).error())
                .isEqualTo(S3ErrorCode.INTERNAL_ERROR);
    }

    @Test
    void s3ExceptionPassesThrough() {
        S3Exception original = new S3Exception(S3ErrorCode.MALFORMED_XML, "bad");
        assertThat(ErrorMapper.toS3(original)).isSameAs(original);
    }
}
