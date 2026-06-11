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
package me.predatorray.candybox.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.client.BoxClient;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LiveDashboardData}. Uses {@link InMemoryCoordinationService} for the
 * coordination half and a hand-written {@link FakeBoxClient} for the client half — see
 * {@code candybox-s3-gateway}'s {@code FakeCandyStore} for the precedent. The fake implements
 * {@link BoxClient}, the narrow SPI the unit-under-test depends on, so the test stays decoupled
 * from {@link CandyboxClient}'s router/transport wiring.
 */
class LiveDashboardDataTest {

    @Test
    void clusterReportsOwnersAndOrphans() {
        InMemoryCoordinationService coord = new InMemoryCoordinationService();
        // Register two nodes with human-readable address blobs; ownership is laid out so node 1 owns
        // two boxes and node 2 owns one — the snapshot's ownedBoxCount needs to count accordingly.
        coord.registerMember(1, "host-a:9709".getBytes(StandardCharsets.UTF_8));
        coord.registerMember(2, "host-b:9709".getBytes(StandardCharsets.UTF_8));
        for (String box : List.of("photos", "docs", "backups", "orphan")) {
            coord.create(CandyboxKeys.boxMetaKey(box), new BoxDescriptor(1).encode());
        }
        coord.tryAcquireLease(CandyboxKeys.ownerResource("photos", 0), 1, 60_000);
        coord.tryAcquireLease(CandyboxKeys.ownerResource("docs", 0), 1, 60_000);
        coord.tryAcquireLease(CandyboxKeys.ownerResource("backups", 0), 2, 60_000);

        FakeBoxClient client = new FakeBoxClient();
        client.boxes.addAll(List.of("photos", "docs", "backups", "orphan"));

        LiveDashboardData data = new LiveDashboardData(coord, client);
        DashboardData.ClusterSnapshot snapshot = data.cluster();

        assertThat(snapshot.boxCount()).isEqualTo(4);
        assertThat(snapshot.ownerless()).containsExactly("orphan");
        assertThat(snapshot.stub()).isFalse();
        Map<String, Integer> owned = new LinkedHashMap<>();
        for (DashboardData.NodeInfo n : snapshot.nodes()) {
            owned.put(n.nodeId(), n.ownedBoxCount());
            assertThat(n.ready()).isTrue();
        }
        assertThat(owned).containsEntry("1", 2).containsEntry("2", 1);
        // Address strings round-trip through UTF-8.
        assertThat(snapshot.nodes()).extracting(DashboardData.NodeInfo::address)
                .containsExactlyInAnyOrder("host-a:9709", "host-b:9709");
    }

    @Test
    void clusterTolerantToEmptyMemberInfo() {
        // A node registered with an empty payload still has to surface (registered ⇒ ready, with
        // address rendered as an empty string instead of throwing). The fake's registerMember
        // requires non-null bytes, so this also pins that contract on the consumer side.
        InMemoryCoordinationService coord = new InMemoryCoordinationService();
        coord.registerMember(7, new byte[0]);
        FakeBoxClient client = new FakeBoxClient();

        LiveDashboardData data = new LiveDashboardData(coord, client);
        DashboardData.ClusterSnapshot snapshot = data.cluster();
        assertThat(snapshot.nodes()).singleElement().satisfies(n -> {
            assertThat(n.nodeId()).isEqualTo("7");
            assertThat(n.address()).isEmpty();
            assertThat(n.ownedBoxCount()).isZero();
        });
    }

    @Test
    void safeListBoxesDegradesGracefullyOnClientFailure() {
        // The dashboard must still answer when the cluster's listBoxes blows up — log + degrade.
        InMemoryCoordinationService coord = new InMemoryCoordinationService();
        coord.registerMember(1, "host:9709".getBytes(StandardCharsets.UTF_8));
        FakeBoxClient client = new FakeBoxClient();
        client.failListBoxes = new RuntimeException("simulated cluster outage");

        LiveDashboardData data = new LiveDashboardData(coord, client);
        // cluster() and boxes() and lsm() all funnel through safeListBoxes — none should throw.
        assertThat(data.cluster().boxCount()).isZero();
        assertThat(data.boxes()).isEmpty();
        assertThat(data.lsm()).isEmpty();
    }

    @Test
    void boxesAndLsmReadOwnerAndManifestVersion() throws Exception {
        InMemoryCoordinationService coord = new InMemoryCoordinationService();
        coord.create(CandyboxKeys.boxMetaKey("photos"), new BoxDescriptor(1).encode());
        coord.create(CandyboxKeys.boxMetaKey("orphan"), new BoxDescriptor(1).encode());
        coord.tryAcquireLease(CandyboxKeys.ownerResource("photos", 0), 4, 60_000);
        // Bumping the manifest pointer twice gives us a deterministic version (2 after create + CAS).
        long v0 = coord.create(CandyboxKeys.manifestKey("photos", 0), new byte[] {1});
        long v1 = coord.compareAndSet(CandyboxKeys.manifestKey("photos", 0), new byte[] {2}, v0);

        FakeBoxClient client = new FakeBoxClient();
        client.boxes.add("photos");
        client.boxes.add("orphan"); // no owner / no manifest — both fields should fall through to -1
        LiveDashboardData data = new LiveDashboardData(coord, client);

        // boxes() yields one row per known box; owner reflects the lease holder when set.
        assertThat(data.boxes()).extracting(DashboardData.BoxSummary::name)
                .containsExactly("photos", "orphan");
        assertThat(data.boxes().get(0).owner()).isEqualTo("4");
        assertThat(data.boxes().get(1).owner()).isNull();

        // box(name) consults headBox first; missing → null even if listBoxes had echoed it.
        client.knownBoxes.add("photos");
        assertThat(data.box("photos")).isNotNull();
        assertThat(data.box("photos").owner()).isEqualTo("4");
        assertThat(data.box("ghost")).isNull();

        // lsm() exposes the manifest version (v1 = post-CAS) and the lease's fencing token; orphan
        // gets -1 / -1 because neither the lease nor the manifest pointer exists.
        List<DashboardData.LsmRow> lsm = data.lsm();
        assertThat(lsm).hasSize(2);
        DashboardData.LsmRow photos = lsm.get(0);
        assertThat(photos.box()).isEqualTo("photos/0");
        assertThat(photos.owner()).isEqualTo("4");
        assertThat(photos.manifestVersion()).isEqualTo(v1);
        assertThat(photos.fencingToken()).isGreaterThan(0);
        // Runtime fields stay sentinels until per-node JSON lands (TODO(phase-5.5)).
        assertThat(photos.sstableLedgerCount()).isEqualTo(-1);

        DashboardData.LsmRow orphan = lsm.get(1);
        assertThat(orphan.owner()).isNull();
        assertThat(orphan.manifestVersion()).isEqualTo(-1);
        assertThat(orphan.fencingToken()).isEqualTo(-1);
    }

    @Test
    void mutatingOpsDelegateToClient() {
        InMemoryCoordinationService coord = new InMemoryCoordinationService();
        FakeBoxClient client = new FakeBoxClient();
        LiveDashboardData data = new LiveDashboardData(coord, client);

        data.createBox("sweets");
        data.putCandy("sweets", "lolly", new byte[] {7, 8, 9}, "application/octet-stream");
        data.deleteCandy("sweets", "lolly");
        data.deleteBox("sweets", true);

        assertThat(client.createdBoxes).containsExactly("sweets");
        // putCandy hands through a null idempotency token + empty user-metadata — those are the
        // defaults the dashboard relies on so the call site stays trivial.
        assertThat(client.puts).singleElement().satisfies(p -> {
            assertThat(p.box).isEqualTo("sweets");
            assertThat(p.key).isEqualTo("lolly");
            assertThat(p.contentType).isEqualTo("application/octet-stream");
            assertThat(p.userMetadata).isEmpty();
            assertThat(p.idempotencyToken).isNull();
        });
        assertThat(client.deletedCandies).containsExactly("sweets/lolly");
        assertThat(client.deletedBoxes).containsExactly("sweets|true");
    }

    @Test
    void candiesListingClampsMaxKeys() {
        InMemoryCoordinationService coord = new InMemoryCoordinationService();
        FakeBoxClient client = new FakeBoxClient();

        // Capture the maxKeys the LSM-level call site receives so we can pin the clamp rules:
        //   maxKeys <= 0       → defaults to 100
        //   maxKeys > 1000     → defaults to 100
        //   otherwise          → forwarded unchanged
        client.listing = new CandyboxClient.Listing(
                List.of(new CandyboxClient.Listing.Entry("k", 1, 2L)), "next");

        LiveDashboardData data = new LiveDashboardData(coord, client);

        DashboardData.CandyListing zero = data.candies("box", "p", "after", 0);
        assertThat(client.lastMaxKeys).isEqualTo(100);
        assertThat(zero.entries()).singleElement().satisfies(e -> {
            assertThat(e.key()).isEqualTo("k");
            assertThat(e.contentLength()).isEqualTo(1);
            assertThat(e.createdAtMillis()).isEqualTo(2L);
        });
        assertThat(zero.nextStartAfter()).isEqualTo("next");

        data.candies("box", null, null, 10_000);
        assertThat(client.lastMaxKeys).isEqualTo(100);

        data.candies("box", null, null, 50);
        assertThat(client.lastMaxKeys).isEqualTo(50);
    }

    // ---- hand-written BoxClient fake ---------------------------------------------------------

    /**
     * Hand-written {@link BoxClient} fake. Records every call so the test can pin both the
     * arguments the dashboard hands down (e.g. that putCandy gets an empty user-metadata map and
     * null idempotency token) and the surface behaviour of the LiveDashboardData layer above.
     */
    private static final class FakeBoxClient implements BoxClient {
        final List<String> boxes = new ArrayList<>();
        final List<String> knownBoxes = new ArrayList<>();
        final List<String> createdBoxes = new ArrayList<>();
        final List<String> deletedBoxes = new ArrayList<>();
        final List<String> deletedCandies = new ArrayList<>();
        final List<RecordedPut> puts = new ArrayList<>();
        RuntimeException failListBoxes;
        CandyboxClient.Listing listing = new CandyboxClient.Listing(List.of(), null);
        int lastMaxKeys = -1;

        @Override
        public List<String> listBoxes() {
            if (failListBoxes != null) {
                throw failListBoxes;
            }
            return List.copyOf(boxes);
        }

        @Override
        public boolean headBox(String box) {
            return knownBoxes.contains(box);
        }

        @Override
        public void createBox(String box) {
            createdBoxes.add(box);
        }

        @Override
        public void deleteBox(String box, boolean force) {
            deletedBoxes.add(box + "|" + force);
        }

        @Override
        public void putCandy(String box, String key, byte[] data, String contentType,
                             Map<String, String> userMetadata, String idempotencyToken) {
            puts.add(new RecordedPut(box, key, data, contentType, userMetadata, idempotencyToken));
        }

        @Override
        public void deleteCandy(String box, String key) {
            deletedCandies.add(box + "/" + key);
        }

        @Override
        public CandyboxClient.Listing listCandies(String box, String prefix, String startAfter,
                                                  int maxKeys) {
            lastMaxKeys = maxKeys;
            return listing;
        }

        record RecordedPut(String box, String key, byte[] data, String contentType,
                           Map<String, String> userMetadata, String idempotencyToken) {
        }
    }
}
