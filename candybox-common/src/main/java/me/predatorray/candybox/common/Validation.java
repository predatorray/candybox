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
