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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class EmptyDashboardDataTest {

    @Test
    void readShapesAreEmptyAndStubFlagIsSet() {
        // The stub flag is the dashboard's hint to render "not wired up". Without it the UI would
        // happily render "0 nodes / 0 boxes" as if the cluster really were empty.
        EmptyDashboardData data = new EmptyDashboardData();
        DashboardData.ClusterSnapshot s = data.cluster();
        assertThat(s.stub()).isTrue();
        assertThat(s.nodes()).isEmpty();
        assertThat(s.ownerless()).isEmpty();
        assertThat(s.boxCount()).isZero();

        assertThat(data.boxes()).isEmpty();
        assertThat(data.lsm()).isEmpty();
        assertThat(data.box("anything")).isNull();
        DashboardData.CandyListing listing = data.candies("box", null, null, 100);
        assertThat(listing.entries()).isEmpty();
        assertThat(listing.nextStartAfter()).isNull();
    }

    @Test
    void mutatingOpsThrowUnsupportedSoServerCanMapTo503() {
        // The contract — documented on DashboardData's default methods — is that the stub backend
        // signals "not supported" by throwing UnsupportedOperationException; AdminApiServer maps
        // that to a 503 rather than a silent 200.
        EmptyDashboardData data = new EmptyDashboardData();
        assertThatThrownBy(() -> data.createBox("x"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("createBox");
        assertThatThrownBy(() -> data.deleteBox("x", false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("deleteBox");
        assertThatThrownBy(() -> data.putCandy("x", "k", new byte[0], null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("putCandy");
        assertThatThrownBy(() -> data.deleteCandy("x", "k"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("deleteCandy");
    }
}
