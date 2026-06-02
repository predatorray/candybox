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

import java.util.List;

/**
 * Returned by {@link AdminApiMain} when no ZK connection string is supplied: lets the admin server
 * still boot, the UI render, and the API answer with valid (empty) shapes. The cluster snapshot's
 * {@code stub} flag is set so the dashboard can show a "not wired up" hint instead of pretending
 * a cluster is fine when there isn't one.
 */
final class EmptyDashboardData implements DashboardData {

    @Override
    public ClusterSnapshot cluster() {
        return new ClusterSnapshot(List.of(), 0, List.of(), true);
    }

    @Override
    public List<BoxSummary> boxes() {
        return List.of();
    }

    @Override
    public BoxSummary box(String name) {
        return null;
    }

    @Override
    public CandyListing candies(String name, String prefix, String startAfter, int maxKeys) {
        return new CandyListing(List.of(), null);
    }

    @Override
    public List<LsmRow> lsm() {
        return List.of();
    }
}
