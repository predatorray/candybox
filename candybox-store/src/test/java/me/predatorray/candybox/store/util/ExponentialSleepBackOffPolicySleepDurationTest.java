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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ExponentialSleepBackOffPolicySleepDurationTest {

    private final ExponentialSleepBackOffPolicy sut;
    private final int collisions;
    private final Long expectedSleepDurationInMillis;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {new ExponentialSleepBackOffPolicy(1, Long.MAX_VALUE), 0, null},
                {new ExponentialSleepBackOffPolicy(1, Long.MAX_VALUE), 1, 1L},
                {new ExponentialSleepBackOffPolicy(1, Long.MAX_VALUE), 2, 2L},
                {new ExponentialSleepBackOffPolicy(1, Long.MAX_VALUE), 3, 4L},
                {new ExponentialSleepBackOffPolicy(1, Long.MAX_VALUE), 4, 8L},
                {new ExponentialSleepBackOffPolicy(3, Long.MAX_VALUE), 5, 48L},
                {new ExponentialSleepBackOffPolicy(2, 7L), 1, 2L},
                {new ExponentialSleepBackOffPolicy(1, 7L), 4, 7L},
                {new ExponentialSleepBackOffPolicy(1, 7L), 10, 7L},
                {new ExponentialSleepBackOffPolicy(1, 7L), 65, 7L},
        });
    }

    public ExponentialSleepBackOffPolicySleepDurationTest(ExponentialSleepBackOffPolicy sut, int collisions,
                                                          Long expectedSleepDurationInMillis) {
        this.sut = sut;
        this.collisions = collisions;
        this.expectedSleepDurationInMillis = expectedSleepDurationInMillis;
    }

    @Test(timeout = 500L)
    public void exercise() throws Exception {
        SutStubWrapper sutStubWrapper = new SutStubWrapper(sut);
        ExponentialSleepBackOffPolicy.Context context = sutStubWrapper.start();
        for (int i = 0; i < collisions; i++) {
            sutStubWrapper.backOff(context);
        }
        assertEquals(expectedSleepDurationInMillis, sutStubWrapper.lastSleepMillis);
    }

    private static class SutStubWrapper extends ExponentialSleepBackOffPolicy {

        Long lastSleepMillis = null;

        SutStubWrapper(ExponentialSleepBackOffPolicy sut) {
            super(sut.initialIntervalInMillis, sut.maximumIntervalInMillis);
        }

        @Override
        void sleep(long millis) {
            this.lastSleepMillis = millis;
        }
    }
}