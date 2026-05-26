package me.predatorray.candybox.lsm.syrup;

import java.util.List;
import me.predatorray.candybox.common.SegmentRef;

/**
 * The result of streaming one Candy's bytes into Syrups.
 *
 * @param segments      the contiguous Syrup segments holding the bytes, in order
 * @param contentLength total bytes written
 * @param crc32c        whole-object end-to-end CRC32C, stored in the CandyLocator for read validation
 */
public record SyrupWriteResult(List<SegmentRef> segments, long contentLength, int crc32c) {
}
