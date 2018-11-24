/*
 * Copyright (c) 2018 the original author or authors.
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

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class IterativeExecutorService implements IterativeExecutor {

    private final Executor executor;

    public IterativeExecutorService(Executor executor) {
        this.executor = executor;
    }

    @Override
    public <T> Future<Object> submit(IterativeRunnable<T> runnable) {
        T initialValue = runnable.initialValue();
        return new Context<>(runnable, null).cont(initialValue);
    }

    private class Context<T> implements IterativeRunnable.Context<T> {

        private final IterativeRunnable<T> runnable;
        private final T current;

        volatile FutureTask<Object> futureTask;

        Context(IterativeRunnable<T> runnable, T current) {
            this.runnable = runnable;
            this.current = current;
        }

        @Override
        public Future<Object> cont(T next) {
            final Context<T> context = new Context<>(runnable, next);
            this.futureTask = new FutureTask<>(() -> {
                Exception ex = null;
                try {
                    runnable.run(context);
                } catch (Exception e) {
                    ex = e;
                }
                if (context.futureTask != null) {
                    return context.futureTask;
                }
                if (ex != null) {
                    throw ex;
                }
                return null;
            });
            executor.execute(futureTask);
            return this.futureTask;
        }

        @Override
        public T current() {
            return current;
        }
    }
}
