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

import java.util.concurrent.ExecutionException;

public class Retry {

    private final ExceptionalRunnable<Exception> r;
    private final BackOffPolicy policy;

    public Retry(ExceptionalRunnable<Exception> r, BackOffPolicy policy) {
        this.r = r;
        this.policy = policy;
    }

    public void runAndRetry() throws InterruptedException, ExecutionException {
        Exception cause;

        BackOffPolicy.Context ctx = policy.start();
        while (true) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            try {
                r.run();
                return;
            } catch (Exception e) {
                if (!policy.backOff(ctx)) {
                    cause = e;
                    break;
                }
            }
        }

        throw new ExecutionException(cause);
    }
}
