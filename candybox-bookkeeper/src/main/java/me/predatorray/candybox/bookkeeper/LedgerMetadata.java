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
package me.predatorray.candybox.bookkeeper;

import java.util.HashMap;
import java.util.Map;

/** Creation metadata read without opening, recovering, or fencing a ledger. */
public record LedgerMetadata(long createdAtMillis, Map<String, byte[]> customMetadata) {
    public LedgerMetadata {
        customMetadata = copy(customMetadata);
    }

    @Override
    public Map<String, byte[]> customMetadata() {
        return copy(customMetadata);
    }

    private static Map<String, byte[]> copy(Map<String, byte[]> source) {
        Map<String, byte[]> result = new HashMap<>();
        source.forEach((key, value) -> result.put(key, value.clone()));
        return Map.copyOf(result);
    }
}
