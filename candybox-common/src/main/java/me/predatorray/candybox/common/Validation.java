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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.LimitExceededException;

/**
 * Centralized size-limit validation, applied both at the client (fail fast) and at the node
 * (authoritative). Throws {@link LimitExceededException} on violation.
 */
public final class Validation {

    private Validation() {
    }

    public static void checkCandyKey(CandyKey key, SizeLimits limits) {
        if (key.byteLength() > limits.maxCandyKeyBytes()) {
            throw new LimitExceededException("CandyKey is " + key.byteLength()
                    + " UTF-8 bytes, exceeds limit of " + limits.maxCandyKeyBytes());
        }
    }

    public static void checkUserMetadata(Map<String, String> metadata, SizeLimits limits) {
        if (metadata == null) {
            return;
        }
        long total = 0;
        for (Map.Entry<String, String> e : metadata.entrySet()) {
            total += e.getKey().getBytes(StandardCharsets.UTF_8).length;
            total += e.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        if (total > limits.maxUserMetadataBytes()) {
            throw new LimitExceededException("User metadata is " + total
                    + " bytes, exceeds limit of " + limits.maxUserMetadataBytes());
        }
    }

    public static void checkCandySize(long contentLength, SizeLimits limits) {
        if (!limits.isCandySizeAllowed(contentLength)) {
            throw new LimitExceededException("Candy size " + contentLength
                    + " exceeds configured maximum of " + limits.maxCandySizeBytes());
        }
    }
}
