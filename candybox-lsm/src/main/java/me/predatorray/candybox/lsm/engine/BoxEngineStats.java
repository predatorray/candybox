package me.predatorray.candybox.lsm.engine;

/**
 * A point-in-time snapshot of a {@link BoxEngine}'s operational counters, for lightweight observability
 * (logging, ops endpoints). Counts are cumulative since the engine was created/recovered.
 *
 * @param puts            successful {@code putCandy} operations
 * @param deletes         {@code deleteCandy} operations
 * @param gets            successful {@code getCandy} operations
 * @param heads           successful {@code headCandy} operations
 * @param lists           {@code listCandies} operations
 * @param flushes         memtable flushes to L0 SSTables
 * @param compactions     committed compaction edits applied
 * @param stallRejections writes rejected with {@code BUSY} under write-stall backpressure
 */
public record BoxEngineStats(long puts, long deletes, long gets, long heads, long lists, long flushes,
                             long compactions, long stallRejections) {
}
