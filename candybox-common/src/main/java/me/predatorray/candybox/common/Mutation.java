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
package me.predatorray.candybox.common;

/**
 * A single LSM mutation: a CandyKey bound to a {@link CandyLocator} (PUT or DELETE). This is the unit
 * appended to the WAL, held in the memtable, and stored in SSTable data blocks.
 *
 * @param key     the CandyKey
 * @param locator the locator (carries the HLC and type)
 */
public record Mutation(CandyKey key, CandyLocator locator) {

    public Mutation {
        if (key == null || locator == null) {
            throw new IllegalArgumentException("key and locator are required");
        }
    }

    public Hlc hlc() {
        return locator.hlc();
    }

    public boolean isTombstone() {
        return locator.isTombstone();
    }
}
