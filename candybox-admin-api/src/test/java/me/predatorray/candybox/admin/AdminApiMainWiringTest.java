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
package me.predatorray.candybox.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AdminApiMainWiringTest {

    @Test
    void missingOrBlankZooKeeperUsesDemoDataWithoutResources() {
        Deque<AutoCloseable> stack = new ArrayDeque<>();
        assertThat(AdminApiMain.buildDataSource(null, stack, Map.of()))
                .isInstanceOf(EmptyDashboardData.class);
        assertThat(stack).isEmpty();
        assertThat(AdminApiMain.buildDataSource("  ", stack, Map.of()))
                .isInstanceOf(EmptyDashboardData.class);
        assertThat(stack).isEmpty();
    }

    @Test
    void closesInReverseOrderAndContinuesAfterFailure() {
        Deque<AutoCloseable> stack = new ArrayDeque<>();
        List<String> closed = new ArrayList<>();
        stack.push(() -> closed.add("first"));
        stack.push(() -> {
            closed.add("second");
            throw new Exception("close failed");
        });
        stack.push(() -> closed.add("third"));

        assertThatCode(() -> AdminApiMain.closeAll(stack)).doesNotThrowAnyException();
        assertThat(closed).containsExactly("third", "second", "first");
        assertThat(stack).isEmpty();
        assertThatCode(() -> AdminApiMain.closeAll(stack)).doesNotThrowAnyException();
    }
}
