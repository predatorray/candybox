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

import me.predatorray.candybox.util.Objects;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class IterativeExecutorServiceTest {

    private ExecutorService executorService;
    private IterativeExecutorService sut;

    @Before
    public void setUp() {
        executorService = Executors.newCachedThreadPool();
        sut = new IterativeExecutorService(executorService);
    }

    @After
    public void tearDown() {
        executorService.shutdown();
    }

    private void awaitAllFutureTermination(Future<Object> future) throws InterruptedException, ExecutionException {
        Future<Object> current = future;
        while (current != null) {
            current = Objects.cast(current.get());
        }
    }

    @Test(timeout = 1000L)
    public void accumulateFrom1To100() throws Exception {
        final int[] result = new int[1];
        class AccumulateFrom1To100 implements IterativeRunnable<Integer> {

            @Override
            public Integer initialValue() {
                return 1;
            }

            @Override
            public void run(Context<Integer> context) {
                Integer i = context.current();
                if (i > 100) {
                    return;
                }
                result[0] += i;
                context.cont(i + 1);
            }
        }
        Future<Object> future = sut.submit(new AccumulateFrom1To100());
        awaitAllFutureTermination(future);
        assertEquals(5050, result[0]);
    }

    @Test(timeout = 1000L, expected = IllegalArgumentException.class)
    public void exceptionDoesNotBlockExecutionAndOnlyLastOneIsThrown() throws Throwable {
        class ProblematicRunnable implements IterativeRunnable<Integer> {

            private final int iterations;

            ProblematicRunnable(int iterations) {
                this.iterations = iterations;
            }

            @Override
            public Integer initialValue() {
                return 1;
            }

            @Override
            public void run(Context<Integer> context) {
                Integer current = context.current();
                if (current <= iterations) {
                    context.cont(current + 1);
                    throw new IllegalStateException(); // always throw exception at the end
                }
                throw new IllegalArgumentException(); // the last exception thrown is IllegalArgument
            }
        }
        Future<Object> future = sut.submit(new ProblematicRunnable(5));
        try {
            awaitAllFutureTermination(future);
        } catch (ExecutionException executionEx) {
            throw executionEx.getCause(); // expect the last exception thrown
        }
    }
}
