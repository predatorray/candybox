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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.VersionedValue;

/**
 * Box ACL documents in the coordination service ({@code acls/<box>}), with a small read-through
 * TTL cache so the per-request authorization check does not hit ZooKeeper every time — the same
 * trade the client's router cache makes ({@code routerCacheTtlMillis}-style staleness, here a
 * fixed {@value #CACHE_TTL_MILLIS} ms). Writes go through compare-and-set and invalidate the cache.
 */
final class BoxAclStore {

    private static final long CACHE_TTL_MILLIS = 5_000;
    private static final int CAS_RETRIES = 3;

    private final CoordinationService coordination;
    private final Clock clock;
    private final ConcurrentMap<String, CachedAcl> cache = new ConcurrentHashMap<>();

    private record CachedAcl(Optional<BoxAcl> acl, long expiresAtMillis) {
    }

    BoxAclStore(CoordinationService coordination, Clock clock) {
        this.coordination = coordination;
        this.clock = clock;
    }

    /** The Box's ACL document, or empty when none was ever written (legacy Box). */
    Optional<BoxAcl> get(String box) {
        long now = clock.currentTimeMillis();
        CachedAcl cached = cache.get(box);
        if (cached != null && now < cached.expiresAtMillis) {
            return cached.acl;
        }
        Optional<BoxAcl> acl = coordination.get(CandyboxKeys.boxAclKey(box))
                .map(v -> BoxAcl.fromBytes(v.value()));
        cache.put(box, new CachedAcl(acl, now + CACHE_TTL_MILLIS));
        return acl;
    }

    /** Creates or replaces the Box's ACL document (CAS with a short retry on races). */
    void set(String box, BoxAcl acl) {
        String key = CandyboxKeys.boxAclKey(box);
        byte[] bytes = acl.toBytes();
        for (int attempt = 0; ; attempt++) {
            try {
                Optional<VersionedValue> current = coordination.get(key);
                if (current.isEmpty()) {
                    coordination.create(key, bytes);
                } else {
                    coordination.compareAndSet(key, bytes, current.get().version());
                }
                cache.remove(box);
                return;
            } catch (CasConflictException raced) {
                if (attempt == CAS_RETRIES) {
                    throw new CandyboxException("Concurrent ACL update on Box " + box, raced);
                }
            }
        }
    }

    /** Writes the initial ACL for a fresh Box; loses quietly if one already exists. */
    void seed(String box, BoxAcl acl) {
        try {
            coordination.create(CandyboxKeys.boxAclKey(box), acl.toBytes());
            cache.remove(box);
        } catch (CasConflictException alreadySeeded) {
            // a concurrent creator won; their ACL stands
        }
    }

    /** Drops the Box's ACL document when the Box is deleted; absent is fine. */
    void delete(String box) {
        String key = CandyboxKeys.boxAclKey(box);
        for (int attempt = 0; ; attempt++) {
            try {
                Optional<VersionedValue> current = coordination.get(key);
                if (current.isPresent()) {
                    coordination.delete(key, current.get().version());
                }
                cache.remove(box);
                return;
            } catch (CasConflictException raced) {
                if (attempt == CAS_RETRIES) {
                    return; // someone else is mutating it concurrently with the delete; let it be
                }
            }
        }
    }
}
