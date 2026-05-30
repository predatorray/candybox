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

import java.util.Locale;
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

/**
 * Translates a thrown {@link Throwable} into an {@link S3Exception} (HTTP status + S3 code). See
 * {@code S3_GATEWAY_PLAN.md} §9 for the mapping table.
 *
 * <p>Two things make this fiddlier than a straight {@code instanceof} switch:
 * <ul>
 *   <li>{@link LimitExceededException} is a {@link ValidationException}, so it must be checked first;
 *       and a bare {@code ValidationException} may be a bad bucket name (from {@code BoxName.of}) vs a
 *       generic bad argument, distinguished by message.</li>
 *   <li>The Candybox <em>client</em> collapses most server-side typed errors into a generic
 *       {@code CandyboxException} whose message is {@code "<SimpleName>: <msg>"} (the server sends the
 *       exception's simple name as the error type). So for a generic {@code CandyboxException} we sniff
 *       that leading type token to recover {@code BoxAlreadyExists}, {@code BoxNotEmpty}, etc.</li>
 * </ul>
 */
final class ErrorMapper {

    private ErrorMapper() {
    }

    static S3Exception toS3(Throwable t) {
        if (t instanceof S3Exception s3) {
            return s3;
        }
        if (t instanceof LimitExceededException e) {
            String msg = lower(e.getMessage());
            S3ErrorCode code = msg.contains("key") ? S3ErrorCode.KEY_TOO_LONG : S3ErrorCode.ENTITY_TOO_LARGE;
            return new S3Exception(code, e.getMessage(), e);
        }
        if (t instanceof ValidationException e) {
            S3ErrorCode code = lower(e.getMessage()).contains("box name")
                    ? S3ErrorCode.INVALID_BUCKET_NAME : S3ErrorCode.INVALID_ARGUMENT;
            return new S3Exception(code, e.getMessage(), e);
        }
        if (t instanceof CandyNotFoundException e) {
            return new S3Exception(S3ErrorCode.NO_SUCH_KEY, e.getMessage(), e);
        }
        if (t instanceof BoxNotFoundException e) {
            return new S3Exception(S3ErrorCode.NO_SUCH_BUCKET, e.getMessage(), e);
        }
        if (t instanceof BoxNotEmptyException e) {
            return new S3Exception(S3ErrorCode.BUCKET_NOT_EMPTY, e.getMessage(), e);
        }
        if (t instanceof BoxAlreadyExistsException e) {
            return new S3Exception(S3ErrorCode.BUCKET_ALREADY_OWNED_BY_YOU, e.getMessage(), e);
        }
        if (t instanceof BusyException e) {
            return new S3Exception(S3ErrorCode.SLOW_DOWN, e.getMessage(), e);
        }
        if (t instanceof NotOwnerException || t instanceof FencedException) {
            // Ownership is in flux (handover/fencing). Retryable; the client re-drives to the new owner.
            return new S3Exception(S3ErrorCode.SERVICE_UNAVAILABLE, t.getMessage(), t);
        }
        if (t instanceof CandyboxException e) {
            return fromGenericMessage(e);
        }
        return new S3Exception(S3ErrorCode.INTERNAL_ERROR, "Internal error", t);
    }

    /**
     * Maps a generic {@link CandyboxException} by sniffing the leading {@code "<TypeName>: "} token the
     * client prepended from the server's error type.
     */
    private static S3Exception fromGenericMessage(CandyboxException e) {
        String message = e.getMessage() == null ? "" : e.getMessage();
        String type = message.indexOf(':') > 0 ? message.substring(0, message.indexOf(':')) : message;
        S3ErrorCode code = switch (type.trim()) {
            case "BoxAlreadyExistsException" -> S3ErrorCode.BUCKET_ALREADY_OWNED_BY_YOU;
            case "BoxNotEmptyException" -> S3ErrorCode.BUCKET_NOT_EMPTY;
            case "BoxNotFoundException" -> S3ErrorCode.NO_SUCH_BUCKET;
            case "CandyNotFoundException" -> S3ErrorCode.NO_SUCH_KEY;
            case "ValidationException" -> S3ErrorCode.INVALID_ARGUMENT;
            case "LimitExceededException" -> S3ErrorCode.ENTITY_TOO_LARGE;
            case "UnsupportedOperation" -> S3ErrorCode.NOT_IMPLEMENTED;
            case "ProtocolError" -> S3ErrorCode.INVALID_REQUEST;
            // The client collapses a server NotFoundResponse to the bare message "Not found"; in the S3
            // surface that is overwhelmingly a missing key/bucket, so prefer 404 over 500.
            default -> lower(message).contains("not found")
                    ? S3ErrorCode.NO_SUCH_KEY : S3ErrorCode.INTERNAL_ERROR;
        };
        return new S3Exception(code, message, e);
    }

    private static String lower(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT);
    }
}
