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
package me.predatorray.candybox.server;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerMetadata;
import me.predatorray.candybox.bookkeeper.LedgerNotFoundException;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.StorageException;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationService;

/** Durable allocation epochs and a single CAS authority for retention pins and deletion claims. */
public final class LedgerRetention {
    static final int MAX_REGISTERED_WRITERS = 4096;
    static final String ROOT = "retention";
    static final String EPOCH_TAG = "candybox-allocation-epoch";
    private final LedgerStore store;
    private final CoordinationService coordination;
    private final Clock clock;

    public LedgerRetention(LedgerStore store, CoordinationService coordination, Clock clock) {
        this.store = store;
        this.coordination = coordination;
        this.clock = clock;
    }

    ScopedStore allocate(String box, String incarnation, int partition) {
        long token = coordination.children(ROOT + "/epochs").stream().map(this::epoch)
                .filter(e -> e.box().equals(box) && e.incarnation().equals(incarnation)
                        && e.partition() == partition).mapToLong(Epoch::ownerToken).max().orElse(0) + 1;
        return allocate(box, incarnation, partition, token);
    }

    ScopedStore allocate(String box, String incarnation, int partition, long ownerToken) {
        List<String> predecessors = coordination.children(ROOT + "/epochs");
        String id = UUID.randomUUID().toString();
        Epoch epoch = new Epoch(box, incarnation, partition, clock.currentTimeMillis(), ownerToken, false, false, Set.of());
        coordination.create(epochKey(id), epoch.encode());
        return new ScopedStore(id, epoch, predecessors);
    }

    private static String epochKey(String id) { return ROOT + "/epochs/" + id; }
    private static String ledgerKey(long id) { return ROOT + "/ledgers/" + id; }

    private Epoch epoch(String id) {
        return Epoch.decode(coordination.get(epochKey(id))
                .orElseThrow(() -> new StorageException("Missing allocation epoch " + id)).value());
    }

    /**
     * Backup capture must acquire this durable pin before recording/copying a ledger. Keep it until
     * the backup no longer depends on the live ledger. A claimed ledger rejects a new pin.
     */
    public void pinBackup(String backupId, Collection<Long> ledgerIds) {
        if (backupId.isBlank() || backupId.contains("/")) {
            throw new IllegalArgumentException("Invalid backup id");
        }
        for (long id : ledgerIds) { pin(id, "backup:" + backupId); }
    }

    public void releaseBackup(String backupId, Collection<Long> ledgerIds) {
        for (long id : ledgerIds) { updatePin(id, "backup:" + backupId, false); }
    }

    void pinRelay(String epochId, String token, Collection<Long> ids) {
        if (epoch(epochId).closing()) { throw new FencedException("Relay source epoch closed"); }
        for (long id : ids) { pin(id, "relay:" + epochId + ":" + token); }
    }

    void releaseRelay(String token, Collection<Long> ids) {
        for (long id : ids) {
            var value = coordination.get(ledgerKey(id));
            if (value.isEmpty()) { continue; }
            for (String holder : Pins.decode(value.get().value()).holders()) {
                if (holder.startsWith("relay:") && holder.endsWith(":" + token)) {
                    updatePin(id, holder, false);
                }
            }
        }
    }

    private boolean held(String holder) {
        if (holder.startsWith("backup:")) { return true; }
        if (holder.startsWith("relay:")) {
            String[] parts = holder.split(":", 3);
            if (parts.length != 3) { throw new StorageException("Invalid relay retention root"); }
            Epoch e = epoch(parts[1]);
            return !e.retired() || coordination.get(CandyboxKeys.locatorRelayKey(
                    e.box(), e.partition(), parts[2])).isPresent();
        }
        if (!holder.startsWith("root:") && !holder.startsWith("transfer:")) {
            throw new StorageException("Unknown retention root");
        }
        return !epoch(holder.substring(holder.indexOf(':') + 1)).retired();
    }

    private void pin(long id, String holder) { updatePin(id, holder, true); }

    private void updatePin(long id, String holder, boolean add) {
        for (int attempt = 0; attempt < 16; attempt++) {
            var current = coordination.get(ledgerKey(id));
            Pins pins = current.map(v -> Pins.decode(v.value())).orElse(new Pins(false, Set.of()));
            if (pins.claimed()) {
                if (!add) { return; }
                throw new FencedException("Ledger " + id + " is claimed");
            }
            Set<String> holders = new HashSet<>(pins.holders());
            holders.removeIf(existing -> !held(existing));
            if (add) {
                // Verify existence before publishing the pin, but serialize against GC via its CAS.
                store.readMetadata(id);
                holders.add(holder);
            } else { holders.remove(holder); }
            try {
                byte[] data = new Pins(false, holders).encode();
                if (current.isEmpty()) { coordination.create(ledgerKey(id), data); }
                else { coordination.compareAndSet(ledgerKey(id), data, current.get().version()); }
                return;
            } catch (CasConflictException retry) { /* revalidate against a concurrent claim */ }
        }
        throw new StorageException("Retention pin contention for ledger " + id);
    }

    /** Claim is permanent, so retries and a successor cannot permit late publication. */
    boolean claim(long id) { return claim(id, ""); }

    private boolean claim(long id, String ignoredEpoch) {
        for (int attempt = 0; attempt < 16; attempt++) {
            var current = coordination.get(ledgerKey(id));
            Pins pins = current.map(v -> Pins.decode(v.value())).orElse(new Pins(false, Set.of()));
            if (pins.claimed()) { return true; }
            for (String holder : pins.holders()) {
                if (!holder.equals(ignoredEpoch)
                        && held(holder)) { return false; }
            }
            try {
                byte[] data = new Pins(true, Set.of()).encode();
                if (current.isEmpty()) { coordination.create(ledgerKey(id), data); }
                else { coordination.compareAndSet(ledgerKey(id), data, current.get().version()); }
                return true;
            } catch (CasConflictException retry) { /* a pin won; revalidate */ }
        }
        return false;
    }

    boolean deletePending(me.predatorray.candybox.lsm.engine.BoxEngine engine, long ledger) {
        byte[] tag = engine.sstableLedgerConfig().customMetadata().get(EPOCH_TAG);
        if (tag == null) { return false; }
        return claim(ledger, "root:" + new String(tag, StandardCharsets.UTF_8));
    }

    /** Explains an allocation without mutating its claim, pins, or enumeration cursor. */
    public Decision inspect(long ledgerId, long graceMillis) {
        LedgerMetadata metadata = store.readMetadata(ledgerId);
        Map<String, byte[]> tags = metadata.customMetadata();
        String allocationId = tag(tags, EPOCH_TAG);
        String role = tag(tags, "candybox-role");
        long age = clock.currentTimeMillis() - metadata.createdAtMillis();
        Set<String> roots = coordination.get(ledgerKey(ledgerId))
                .map(v -> Pins.decode(v.value()).holders()).orElse(Set.of());
        String reason;
        if (graceMillis <= 0) { reason = "disabled"; }
        else if (allocationId.isEmpty()) { reason = "legacy allocation without an epoch"; }
        else if (!Set.of("WAL", "MANIFEST", "SSTABLE", "SYRUP").contains(role)) {
            reason = "unknown ledger role";
        } else {
            Epoch e = epoch(allocationId);
            if (!e.box().equals(tag(tags, "candybox-box"))
                    || !e.incarnation().equals(tag(tags, "candybox-incarnation"))) {
                reason = "allocation identity mismatch";
            } else if (!e.retired()) { reason = "writer epoch remains active"; }
            else if (metadata.createdAtMillis() <= 0 || age < graceMillis) { reason = "young or unknown creation time"; }
            else if (roots.stream().anyMatch(this::held)) {
                reason = "retained by a recovery, transfer, or backup root";
            } else { reason = "eligible after epoch fencing and root handoff"; }
        }
        return new Decision(ledgerId, age, tag(tags, "candybox-box"),
                tag(tags, "candybox-incarnation"), role, allocationId, Set.copyOf(roots), reason);
    }

    public record Decision(long ledgerId, long ageMillis, String box, String incarnation,
                           String role, String allocationEpoch, Set<String> knownRoots, String reason) { }

    /** Deleted incarnations have no publication authority; close admission before fencing writers. */
    private void retireDeletedEpochs() {
        for (String id : coordination.children(ROOT + "/epochs")) {
            var current = coordination.get(epochKey(id)).orElseThrow();
            Epoch e = Epoch.decode(current.value());
            if (e.retired()) { continue; }
            var descriptor = coordination.get(CandyboxKeys.boxMetaKey(e.box()));
            if (descriptor.isPresent()
                    && BoxDescriptor.decode(descriptor.get().value()).incarnation().equals(e.incarnation())) {
                continue;
            }
            if (!e.closing()) {
                coordination.compareAndSet(epochKey(id), new Epoch(e.box(), e.incarnation(), e.partition(),
                        e.createdAt(), e.ownerToken(), false, true, e.writers()).encode(), current.version());
                current = coordination.get(epochKey(id)).orElseThrow();
                e = Epoch.decode(current.value());
            }
            for (long writer : e.writers()) {
                try (ReadableLedger fenced = store.recoverOpen(writer)) {
                    // Fences every admitted WAL/manifest, including unpublished CAS candidates.
                } catch (LedgerNotFoundException gone) { /* already deleted */ }
            }
            coordination.compareAndSet(epochKey(id), new Epoch(e.box(), e.incarnation(), e.partition(),
                    e.createdAt(), e.ownerToken(), true, true, e.writers()).encode(), current.version());
        }
    }

    /** A bounded, restartable pass. Legacy allocations and unknown roles are always held. */
    public Report reconcile(long graceMillis, int limit, boolean reportOnly) {
        if (graceMillis <= 0 || limit <= 0) { return new Report(0, 0, 0, 0, 0); }
        long candidates = 0, held = 0, deleted = 0, errors = 0, oldest = 0;
        if (!reportOnly) {
            try { retireDeletedEpochs(); }
            catch (RuntimeException unavailable) { return new Report(0, 0, 0, 1, 0); }
        }
        int examined = 0;
        String cursorKey = ROOT + "/enumeration-cursor";
        var cursor = coordination.get(cursorKey);
        long after = cursor.map(v -> decodeCursor(v.value())).orElse(-1L);
        List<Long> ids = store.listLedgers().stream().sorted().toList();
        List<Long> remaining = ids.stream().filter(id -> id > after).toList();
        if (remaining.isEmpty()) { remaining = ids; }
        long last = -1;
        for (long id : remaining) {
            if (examined++ >= limit) { break; }
            last = id;
            try {
                LedgerMetadata metadata = store.readMetadata(id);
                Map<String, byte[]> tags = metadata.customMetadata();
                String allocationId = tag(tags, EPOCH_TAG);
                String role = tag(tags, "candybox-role");
                if (allocationId.isEmpty() || !Set.of("WAL", "MANIFEST", "SSTABLE", "SYRUP").contains(role)) {
                    held++; continue;
                }
                Epoch e = epoch(allocationId);
                long age = clock.currentTimeMillis() - metadata.createdAtMillis();
                if (!e.retired() || !e.box().equals(tag(tags, "candybox-box"))
                        || !e.incarnation().equals(tag(tags, "candybox-incarnation"))
                        || metadata.createdAtMillis() <= 0 || age < graceMillis) {
                    held++; continue;
                }
                candidates++;
                oldest = Math.max(oldest, age);
                if (!reportOnly && claim(id)) {
                    store.deleteLedger(id);
                    deleted++;
                } else { held++; }
            } catch (LedgerNotFoundException raced) {
                // Another idempotent collector completed this allocation.
            } catch (RuntimeException failure) { errors++; }
        }
        if (!reportOnly && last >= 0) {
            byte[] next = new BinaryWriter().writeByte(1).writeLong(last).toByteArray();
            try {
                if (cursor.isEmpty()) { coordination.create(cursorKey, next); }
                else { coordination.compareAndSet(cursorKey, next, cursor.get().version()); }
            } catch (CasConflictException raced) { /* a successor made progress */ }
        }
        return new Report(candidates, held, deleted, errors, oldest);
    }

    private static long decodeCursor(byte[] bytes) {
        BinaryReader r = new BinaryReader(bytes);
        if (r.readByte() != 1) { throw new StorageException("Unknown enumeration cursor version"); }
        return r.readLong();
    }

    private static String tag(Map<String, byte[]> tags, String key) {
        byte[] value = tags.get(key);
        return value == null ? "" : new String(value, StandardCharsets.UTF_8);
    }

    public record Report(long candidates, long held, long deleted, long errors, long oldestAgeMillis) { }

    record Epoch(String box, String incarnation, int partition, long createdAt, long ownerToken, boolean retired,
                 boolean closing, Set<Long> writers) {
        byte[] encode() {
            BinaryWriter w = new BinaryWriter().writeByte(2).writeString(box).writeString(incarnation)
                    .writeVarInt(partition).writeLong(createdAt).writeVarLong(ownerToken).writeBoolean(retired).writeBoolean(closing)
                    .writeVarInt(writers.size());
            writers.stream().sorted().forEach(w::writeVarLong);
            return w.toByteArray();
        }
        static Epoch decode(byte[] bytes) {
            BinaryReader r = new BinaryReader(bytes);
            int version = r.readByte();
            if (version != 2) { throw new StorageException("Unknown allocation epoch version"); }
            String box = r.readString(), incarnation = r.readString();
            int partition = r.readVarInt();
            long created = r.readLong();
            long token = r.readVarLong();
            boolean retired = r.readBoolean(), closing = r.readBoolean();
            int count = r.readVarInt();
            if (count > bytes.length) { throw new StorageException("Invalid writer count"); }
            Set<Long> writers = new HashSet<>();
            for (int i = 0; i < count; i++) { writers.add(r.readVarLong()); }
            return new Epoch(box, incarnation, partition, created, token, retired, closing, writers);
        }
    }

    private record Pins(boolean claimed, Set<String> holders) {
        byte[] encode() {
            BinaryWriter w = new BinaryWriter().writeByte(1).writeBoolean(claimed).writeVarInt(holders.size());
            holders.stream().sorted().forEach(w::writeString);
            return w.toByteArray();
        }
        static Pins decode(byte[] bytes) {
            BinaryReader r = new BinaryReader(bytes);
            if (r.readByte() != 1) { throw new StorageException("Unknown retention pin version"); }
            boolean claimed = r.readBoolean();
            int count = r.readVarInt();
            if (count > bytes.length) { throw new StorageException("Invalid pin count"); }
            Set<String> holders = new HashSet<>();
            for (int i = 0; i < count; i++) { holders.add(r.readString()); }
            return new Pins(claimed, holders);
        }
    }

    final class ScopedStore implements LedgerStore {
        private final String id;
        private final Epoch allocation;
        private final List<String> predecessors;
        ScopedStore(String id, Epoch allocation, List<String> predecessors) {
            this.id = id;
            this.allocation = allocation;
            this.predecessors = predecessors;
        }

        /** Only after pointer CAS succeeded and recovery fenced the predecessor's manifest and WAL. */
        void inheritAndRetire(Collection<Long> roots) { inheritAndRetire(roots, Set.of()); }

        void inheritAndRetire(Collection<Long> roots, Set<Long> pendingDeletes) {
            for (long root : roots) {
                try { pin(root, "root:" + id); }
                catch (FencedException claimed) {
                    boolean durableClaim = coordination.get(ledgerKey(root))
                            .map(value -> Pins.decode(value.value()).claimed()).orElse(false);
                    if (!pendingDeletes.contains(root) || !durableClaim) { throw claimed; }
                }
                catch (LedgerNotFoundException missing) {
                    if (!pendingDeletes.contains(root)) { throw missing; }
                }
            }
            for (String predecessor : predecessors) {
                var old = coordination.get(epochKey(predecessor)).orElseThrow();
                Epoch e = Epoch.decode(old.value());
                if (!e.retired() && e.box().equals(allocation.box())
                        && e.incarnation().equals(allocation.incarnation())
                        && e.partition() == allocation.partition()
                        && e.ownerToken() < allocation.ownerToken()) {
                    coordination.compareAndSet(epochKey(predecessor), new Epoch(e.box(), e.incarnation(),
                            e.partition(), e.createdAt(), e.ownerToken(), true, true, e.writers()).encode(), old.version());
                }
            }
        }

        void retireDeleted() {
            var current = coordination.get(epochKey(id)).orElseThrow();
            coordination.compareAndSet(epochKey(id), new Epoch(allocation.box(), allocation.incarnation(),
                    allocation.partition(), allocation.createdAt(), allocation.ownerToken(), true, true, Epoch.decode(current.value()).writers()).encode(), current.version());
        }

        @Override public Map<String, byte[]> allocationMetadata() {
            return Map.of(EPOCH_TAG, id.getBytes(StandardCharsets.UTF_8),
                    "candybox-incarnation", allocation.incarnation().getBytes(StandardCharsets.UTF_8),
                    "candybox-partition", Integer.toString(allocation.partition()).getBytes(StandardCharsets.UTF_8));
        }
        @Override public WritableLedger createLedger(LedgerConfig config) {
            if (epoch(id).closing()) { throw new FencedException("Allocation epoch closed"); }
            Map<String, byte[]> metadata = new HashMap<>(config.customMetadata());
            metadata.putAll(allocationMetadata());
            WritableLedger ledger = store.createLedger(new LedgerConfig(config.quorum(), metadata));
            String role = tag(metadata, "candybox-role");
            if (role.equals("MANIFEST") || role.equals("WAL")) {
                try { registerWriter(ledger.ledgerId()); }
                catch (RuntimeException failure) { ledger.close(); throw failure; }
                return new RegisteredWriter(ledger);
            }
            return ledger;
        }
        private void registerWriter(long ledgerId) {
            for (int attempt = 0; attempt < 16; attempt++) {
                var current = coordination.get(epochKey(id)).orElseThrow();
                Epoch e = Epoch.decode(current.value());
                if (e.closing()) { throw new FencedException("Allocation epoch closed"); }
                Set<Long> writers = new HashSet<>(e.writers());
                if (writers.size() >= MAX_REGISTERED_WRITERS) {
                    throw new me.predatorray.candybox.common.exception.BusyException("Allocation epoch needs rotation");
                }
                writers.add(ledgerId);
                try {
                    coordination.compareAndSet(epochKey(id), new Epoch(e.box(), e.incarnation(),
                            e.partition(), e.createdAt(), e.ownerToken(), false, false, writers).encode(), current.version());
                    return;
                } catch (CasConflictException retry) { /* deletion must win or fence this writer */ }
            }
            throw new StorageException("Writer registration contention");
        }

        boolean rotationDue(long graceMillis) {
            return clock.currentTimeMillis() - allocation.createdAt() >= graceMillis
                    || epoch(id).writers().size() >= MAX_REGISTERED_WRITERS / 4;
        }

        private void unregisterSealedWriter(long ledgerId) {
            for (int attempt = 0; attempt < 16; attempt++) {
                var current = coordination.get(epochKey(id)).orElseThrow();
                Epoch e = Epoch.decode(current.value());
                Set<Long> writers = new HashSet<>(e.writers());
                if (!writers.remove(ledgerId)) { return; }
                try {
                    coordination.compareAndSet(epochKey(id), new Epoch(e.box(), e.incarnation(),
                            e.partition(), e.createdAt(), e.ownerToken(), e.retired(), e.closing(), writers)
                            .encode(), current.version());
                    return;
                } catch (CasConflictException retry) { /* sealed ids are safe to retain on contention */ }
            }
        }

        private final class RegisteredWriter implements WritableLedger {
            private final WritableLedger delegate;
            RegisteredWriter(WritableLedger delegate) { this.delegate = delegate; }
            @Override public long append(byte[] data) { return delegate.append(data); }
            @Override public long ledgerId() { return delegate.ledgerId(); }
            @Override public long lastAddConfirmed() { return delegate.lastAddConfirmed(); }
            @Override public boolean isSealed() { return delegate.isSealed(); }
            @Override public Map<String, byte[]> customMetadata() { return delegate.customMetadata(); }
            @Override public me.predatorray.candybox.bookkeeper.LedgerEntry read(long entry) {
                return delegate.read(entry);
            }
            @Override public List<me.predatorray.candybox.bookkeeper.LedgerEntry> readRange(long first, long last) {
                return delegate.readRange(first, last);
            }
            @Override public void close() {
                delegate.close();
                try { unregisterSealedWriter(delegate.ledgerId()); }
                catch (RuntimeException unavailable) {
                    // The sealed id remains a conservative fence root; recovery can retire it later.
                }
            }
        }

        @Override public void retainLedgers(Collection<Long> ids) {
            if (epoch(id).closing()) { throw new FencedException("Allocation epoch closed"); }
            for (long ledger : new HashSet<>(ids)) {
                Map<String, byte[]> tags = store.readMetadata(ledger).customMetadata();
                if (!allocation.box().equals(tag(tags, "candybox-box"))
                        || !"SYRUP".equals(tag(tags, "candybox-role"))
                        || (!tag(tags, "candybox-incarnation").isEmpty()
                            && !allocation.incarnation().equals(tag(tags, "candybox-incarnation")))) {
                    throw new StorageException("Imported ledger is not a Syrup of this Box incarnation");
                }
                pin(ledger, "transfer:" + id);
            }
        }
        @Override public ReadableLedger openLedger(long ledger) { return store.openLedger(ledger); }
        @Override public ReadableLedger recoverOpen(long ledger) { return store.recoverOpen(ledger); }
        @Override public LedgerMetadata readMetadata(long ledger) { return store.readMetadata(ledger); }
        @Override public void deleteLedger(long ledger) { store.deleteLedger(ledger); }
        @Override public List<Long> listLedgers() { return store.listLedgers(); }
        @Override public void close() { /* shared backend */ }
    }
}
