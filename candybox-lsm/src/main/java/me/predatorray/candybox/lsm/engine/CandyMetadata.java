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

import java.util.Map;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;

/**
 * Metadata about a live Candy, returned by {@code headCandy} and as part of {@code putCandy}/
 * {@code getCandy}. Derived from the winning {@link CandyLocator} without reading the bytes.
 *
 * @param contentLength   length in bytes
 * @param contentType     optional content type
 * @param userMetadata    user metadata map
 * @param crc32c          whole-object CRC32C
 * @param hlc             the LWW timestamp of the winning write
 * @param createdAtMillis creation wall-clock time
 */
public record CandyMetadata(
        long contentLength,
        String contentType,
        Map<String, String> userMetadata,
        int crc32c,
        Hlc hlc,
        long createdAtMillis) {

    public static CandyMetadata from(CandyLocator locator) {
        return new CandyMetadata(locator.contentLength(), locator.contentType(),
                locator.userMetadata(), locator.crc32c(), locator.hlc(), locator.createdAtMillis());
    }
}
