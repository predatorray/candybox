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
