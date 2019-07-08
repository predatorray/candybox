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

package me.predatorray.candybox.store.service;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcServerRule;
import me.predatorray.candybox.store.util.ExceptionalRunnable;
import org.junit.Rule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AbstractGrpcService {

    @Rule
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

    protected final <T extends Exception> void assertNotFoundStatusThrown(ExceptionalRunnable<T> runnable)
            throws AssertionError, T {
        Status status;
        try {
            runnable.run();
            fail("Expected exception: io.grpc.StatusRuntimeException");
            return;
        } catch (StatusRuntimeException e) {
            status = e.getStatus();
        }
        assertEquals(Status.Code.NOT_FOUND, status.getCode());
    }
}
