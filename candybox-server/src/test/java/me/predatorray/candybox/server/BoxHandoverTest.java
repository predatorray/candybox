package me.predatorray.candybox.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import org.junit.jupiter.api.Test;

/**
 * Ownership handover across two nodes: lease expiry lets a second node take over a Box, recover its
 * manifest + WAL, advance the manifest pointer, and serve the data — while the old owner is fenced.
 * Deterministic via a {@link ManualClock} driving lease TTL, with the renew heartbeat disabled.
 */
class BoxHandoverTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void secondNodeTakesOverAfterLeaseExpiresAndOldOwnerIsFenced() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        CandyboxConfig config = CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0) // disable the heartbeat for determinism
                .ownershipLeaseTtlMillis(10_000)
                .build();
        BoxName box = BoxName.of("shared-box");

        CandyboxNode nodeA = new CandyboxNode(1, config, store, coordination, clock);
        CandyboxNode nodeB = new CandyboxNode(2, config, store, coordination, clock);
        try {
            nodeA.createBox(box);
            // Unflushed write: lives only in A's WAL, exercising WAL recovery on handover.
            nodeA.engine(box).putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);

            // While A holds a valid lease, B cannot take over.
            assertThatThrownBy(() -> nodeB.openBox(box)).isInstanceOf(NotOwnerException.class);

            // A's lease lapses (no heartbeat); B takes over.
            clock.advance(11_000);
            nodeB.openBox(box);

            assertThat(nodeB.engine(box).getCandy(CandyKey.of("k"))).isEqualTo(bytes("v1"));

            // The old owner is fenced: it no longer owns the Box.
            assertThatThrownBy(() -> nodeA.engine(box)).isInstanceOf(NotOwnerException.class);

            // The manifest pointer now names B's manifest ledger.
            ManifestPointer pointer = ManifestPointer.decode(
                    coordination.get(BoxOwnership.manifestKey(box)).orElseThrow().value());
            assertThat(pointer.ledgerId()).isEqualTo(nodeB.engine(box).manifestLedgerId());
            assertThat(pointer.ownerToken()).isGreaterThan(1L);
        } finally {
            nodeA.close();
            nodeB.close();
            store.close();
        }
    }

    @Test
    void newWriterAfterHandoverWinsLwwDespiteAOlderWrite() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        CandyboxConfig config = CandyboxConfig.builder().leaseRenewIntervalMillis(0).build();
        BoxName box = BoxName.of("lww-box");

        CandyboxNode nodeA = new CandyboxNode(1, config, store, coordination, clock);
        CandyboxNode nodeB = new CandyboxNode(2, config, store, coordination, clock);
        try {
            nodeA.createBox(box);
            nodeA.engine(box).putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);
            clock.advance(11_000);
            nodeB.openBox(box);

            nodeB.engine(box).putCandy(CandyKey.of("k"), bytes("v2"), null, Map.of(), null);
            assertThat(nodeB.engine(box).getCandy(CandyKey.of("k"))).isEqualTo(bytes("v2"));
        } finally {
            nodeA.close();
            nodeB.close();
            store.close();
        }
    }

    @Test
    void nonOwnerHandlerReturnsMovedToTheOwner() {
        ManualClock clock = new ManualClock(1_000);
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        CandyboxConfig config = CandyboxConfig.builder().leaseRenewIntervalMillis(0).build();
        MessageCodec codec = new MessageCodec();
        BoxName box = BoxName.of("routed-box");

        CandyboxNode owner = new CandyboxNode(1, config, store, coordination, clock);
        CandyboxNode other = new CandyboxNode(2, config, store, coordination, clock);
        try {
            owner.createBox(box);

            // A request for the Box that lands on the non-owner is redirected to the owner (node 1).
            Message moved = codec.decode(other.requestHandler()
                    .handle(codec.encode(new Message.GetCandyRequest(box.value(), "k"))));
            assertThat(moved).isInstanceOf(Message.MovedResponse.class);
            assertThat(((Message.MovedResponse) moved).ownerNodeId()).isEqualTo(1);

            // A request for a Box that nobody owns is a plain not-found.
            Message absent = codec.decode(other.requestHandler()
                    .handle(codec.encode(new Message.GetCandyRequest("absent-box", "k"))));
            assertThat(absent).isInstanceOf(Message.NotFoundResponse.class);
        } finally {
            owner.close();
            other.close();
            store.close();
        }
    }
}
