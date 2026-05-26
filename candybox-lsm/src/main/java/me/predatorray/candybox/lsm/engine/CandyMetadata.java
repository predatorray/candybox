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
