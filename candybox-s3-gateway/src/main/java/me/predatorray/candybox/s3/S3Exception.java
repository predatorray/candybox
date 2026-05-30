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
 * A failure that should be rendered to the client as an S3 {@code <Error>} response. Carries the
 * {@link S3ErrorCode} (HTTP status + code) and an optional message overriding the code's default.
 */
final class S3Exception extends RuntimeException {

    private final S3ErrorCode error;

    S3Exception(S3ErrorCode error) {
        this(error, error.defaultMessage(), null);
    }

    S3Exception(S3ErrorCode error, String message) {
        this(error, message, null);
    }

    S3Exception(S3ErrorCode error, String message, Throwable cause) {
        super(message, cause);
        this.error = error;
    }

    S3ErrorCode error() {
        return error;
    }
}
