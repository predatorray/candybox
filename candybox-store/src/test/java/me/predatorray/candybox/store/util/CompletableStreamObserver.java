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

import io.grpc.stub.StreamObserver;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CompletableStreamObserver<T> implements StreamObserver<T> {

    private final LinkedList<T> nextValues = new LinkedList<>();
    private final CompletableFuture<List<T>> cf = new CompletableFuture<>();

    @Override
    public void onNext(T value) {
        nextValues.offer(value);
    }

    @Override
    public void onError(Throwable t) {
        cf.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
        cf.complete(nextValues);
    }

    public CompletableFuture<List<T>> getCompletableFuture() {
        return cf;
    }
}
