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
package me.predatorray.candybox.lsm.engine;

/**
 * A point-in-time snapshot of a {@link BoxEngine}'s operational counters, for lightweight observability
 * (logging, ops endpoints). Counts are cumulative since the engine was created/recovered.
 *
 * @param puts            successful {@code putCandy} operations
 * @param deletes         {@code deleteCandy} operations
 * @param gets            successful {@code getCandy} operations
 * @param heads           successful {@code headCandy} operations
 * @param lists           {@code listCandies} operations
 * @param flushes         memtable flushes to L0 SSTables
 * @param compactions     committed compaction edits applied
 * @param stallRejections writes rejected with {@code BUSY} under write-stall backpressure
 */
public record BoxEngineStats(long puts, long deletes, long gets, long heads, long lists, long flushes,
                             long compactions, long stallRejections) {
}
