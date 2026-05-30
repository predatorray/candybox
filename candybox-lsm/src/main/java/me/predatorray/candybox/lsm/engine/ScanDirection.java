package me.predatorray.candybox.lsm.engine;

/** The order in which a scan walks the sorted keyspace. */
public enum ScanDirection {
    /** Ascending unsigned-byte key order (the default, matching {@code listCandies}). */
    FORWARD,
    /** Descending unsigned-byte key order. */
    REVERSE
}
