package me.predatorray.candybox.coordination;

/**
 * A value plus the version that produced it, the unit of optimistic concurrency for coordination
 * keys (notably the per-Box manifest pointer). The version increments on every successful write and
 * is the {@code expectedVersion} a compare-and-set must match.
 *
 * @param version the current version (starts at 0 on create)
 * @param value   the stored bytes (treat as read-only)
 */
public record VersionedValue(long version, byte[] value) {
}
