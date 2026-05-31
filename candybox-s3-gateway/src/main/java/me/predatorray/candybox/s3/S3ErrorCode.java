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
 * The subset of S3 error codes the gateway emits, each paired with its HTTP status and a default
 * human-readable message. Rendered into the standard S3 {@code <Error>} XML body. See
 * {@code S3_GATEWAY_PLAN.md} §9.
 */
enum S3ErrorCode {
    NO_SUCH_KEY(404, "NoSuchKey", "The specified key does not exist."),
    NO_SUCH_BUCKET(404, "NoSuchBucket", "The specified bucket does not exist."),
    BUCKET_ALREADY_OWNED_BY_YOU(409, "BucketAlreadyOwnedByYou",
            "Your previous request to create the named bucket succeeded and you already own it."),
    BUCKET_NOT_EMPTY(409, "BucketNotEmpty", "The bucket you tried to delete is not empty."),
    INVALID_BUCKET_NAME(400, "InvalidBucketName", "The specified bucket is not valid."),
    INVALID_ARGUMENT(400, "InvalidArgument", "Invalid Argument."),
    INVALID_REQUEST(400, "InvalidRequest", "Invalid Request."),
    KEY_TOO_LONG(400, "KeyTooLongError", "Your key is too long."),
    ENTITY_TOO_LARGE(400, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed object size."),
    INVALID_RANGE(416, "InvalidRange", "The requested range is not satisfiable."),
    MALFORMED_XML(400, "MalformedXML", "The XML you provided was not well-formed or did not validate."),
    METHOD_NOT_ALLOWED(405, "MethodNotAllowed", "The specified method is not allowed against this resource."),
    NOT_IMPLEMENTED(501, "NotImplemented", "A header or operation you provided is not implemented."),
    SLOW_DOWN(503, "SlowDown", "Please reduce your request rate."),
    SERVICE_UNAVAILABLE(503, "ServiceUnavailable", "Reduce your request rate."),
    INTERNAL_ERROR(500, "InternalError", "We encountered an internal error. Please try again.");

    private final int httpStatus;
    private final String code;
    private final String defaultMessage;

    S3ErrorCode(int httpStatus, String code, String defaultMessage) {
        this.httpStatus = httpStatus;
        this.code = code;
        this.defaultMessage = defaultMessage;
    }

    int httpStatus() {
        return httpStatus;
    }

    String code() {
        return code;
    }

    String defaultMessage() {
        return defaultMessage;
    }

    /** Whether a client should retry after backing off (drives the {@code Retry-After} header). */
    boolean retryable() {
        return this == SLOW_DOWN || this == SERVICE_UNAVAILABLE;
    }
}
