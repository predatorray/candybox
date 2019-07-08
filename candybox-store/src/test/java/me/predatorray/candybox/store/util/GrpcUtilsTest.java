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
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GrpcUtilsTest {

    @Test
    public void testSendNotFoundError() {
        StreamObserver observer = mock(StreamObserver.class);
        String errorMessage = "message";
        Exception cause = new Exception();

        GrpcUtils.sendNotFoundError(observer, errorMessage, cause);

        ArgumentCaptor<StatusException> rpcError = ArgumentCaptor.forClass(StatusException.class);
        verify(observer).onError(rpcError.capture());
        StatusException statusException = rpcError.getValue();
        Status status = statusException.getStatus();
        assertEquals(errorMessage, status.getDescription());
        assertEquals(Status.Code.NOT_FOUND, status.getCode());
        assertEquals(cause, status.getCause());
    }

    @Test
    public void testSendNotFoundErrorWithoutMessage() {
        StreamObserver observer = mock(StreamObserver.class);
        Exception cause = new Exception();

        GrpcUtils.sendNotFoundError(observer, null, cause);

        ArgumentCaptor<StatusException> rpcError = ArgumentCaptor.forClass(StatusException.class);
        verify(observer).onError(rpcError.capture());
        StatusException statusException = rpcError.getValue();
        Status status = statusException.getStatus();
        assertNull(status.getDescription());
        assertEquals(Status.Code.NOT_FOUND, status.getCode());
        assertEquals(cause, status.getCause());
    }

    @Test
    public void testSendNotFoundErrorWithoutCause() {
        StreamObserver observer = mock(StreamObserver.class);
        GrpcUtils.sendNotFoundError(observer, null, null);

        ArgumentCaptor<StatusException> rpcError = ArgumentCaptor.forClass(StatusException.class);
        verify(observer).onError(rpcError.capture());
        StatusException statusException = rpcError.getValue();
        Status status = statusException.getStatus();
        assertNull(status.getDescription());
        assertNull(status.getCause());
    }

    @Test
    public void testSendInternalError() {
        StreamObserver observer = mock(StreamObserver.class);
        String errorMessage = "message";
        Exception cause = new Exception();

        GrpcUtils.sendInternalError(observer, errorMessage, cause);

        ArgumentCaptor<StatusException> rpcError = ArgumentCaptor.forClass(StatusException.class);
        verify(observer).onError(rpcError.capture());
        StatusException statusException = rpcError.getValue();
        Status status = statusException.getStatus();
        assertEquals(errorMessage, status.getDescription());
        assertEquals(Status.Code.INTERNAL, status.getCode());
        assertEquals(cause, status.getCause());
    }

    @Test
    public void sendError() {
        StreamObserver observer = mock(StreamObserver.class);
        Status status = Status.UNKNOWN;
        String errorMessage = "message";
        Exception cause = new Exception();

        GrpcUtils.sendError(observer, status, errorMessage, cause);

        ArgumentCaptor<StatusException> rpcError = ArgumentCaptor.forClass(StatusException.class);
        verify(observer).onError(rpcError.capture());
        StatusException statusException = rpcError.getValue();
        Status actualStatus = statusException.getStatus();
        assertEquals(errorMessage, actualStatus.getDescription());
        assertEquals(status.getCode(), actualStatus.getCode());
        assertEquals(cause, actualStatus.getCause());
    }
}