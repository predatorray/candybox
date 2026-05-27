package me.predatorray.candybox.coordination.fake;

import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.CoordinationServiceContract;

/** Runs the shared {@link CoordinationServiceContract} against the in-memory fake. */
class InMemoryCoordinationServiceContractTest extends CoordinationServiceContract {

    @Override
    protected CoordinationService newService(Clock clock) {
        return new InMemoryCoordinationService(clock);
    }
}
