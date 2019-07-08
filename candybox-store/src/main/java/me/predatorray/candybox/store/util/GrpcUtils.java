/*
 * Copyright (c) 2019 the original author or authors.
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

package me.predatorray.candybox.store.util;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

public class GrpcUtils {

    public static final void sendNotFoundError(StreamObserver<?> responseObserver, String errorMessage,
                                               Throwable cause) {
        sendError(responseObserver, Status.NOT_FOUND, errorMessage, cause);
    }

    public static final void sendInternalError(StreamObserver<?> responseObserver, String errorMessage,
                                               Throwable cause) {
        sendError(responseObserver, Status.INTERNAL, errorMessage, cause);
    }

    public static final void sendError(StreamObserver<?> responseObserver, Status status,
                                       String errorMessage, Throwable cause) {
        Objects.requireNonNull(responseObserver, "responseObserver must not be null");
        Objects.requireNonNull(status, "status must not be null");

        if (errorMessage != null) {
            status = status.withDescription(errorMessage);
        }
        if (cause != null) {
            status = status.withCause(cause);
        }
        responseObserver.onError(status.asException());
    }
}
