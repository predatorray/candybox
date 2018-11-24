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

public class ExponentialSleepBackOffPolicy implements BackOffPolicy {

    final long initialIntervalInMillis;
    final long maximumIntervalInMillis;

    public ExponentialSleepBackOffPolicy(long initialIntervalInMillis, long maximumIntervalInMillis) {
        this.initialIntervalInMillis = initialIntervalInMillis;
        this.maximumIntervalInMillis = maximumIntervalInMillis;
    }

    @Override
    public Context start() {
        return new Context();
    }

    @Override
    public void backOff(BackOffPolicy.Context context) throws InterruptedException {
        Context ctx = (Context) context;
        sleep(ctx.getAndIncrementSleepTime());
    }

    // Visible for Testing
    void sleep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    // Visible for Testing
    class Context implements BackOffPolicy.Context {

        long interval = ExponentialSleepBackOffPolicy.this.initialIntervalInMillis;
        int collisions = 0;
        boolean reachedMaximum = false;

        long getAndIncrementSleepTime() {
            if (reachedMaximum) {
                return maximumIntervalInMillis;
            }
            long sleep = interval * (1 << (collisions++));
            if (sleep >= maximumIntervalInMillis) {
                reachedMaximum = true;
                return maximumIntervalInMillis;
            } else {
                return sleep;
            }
        }
    }
}
