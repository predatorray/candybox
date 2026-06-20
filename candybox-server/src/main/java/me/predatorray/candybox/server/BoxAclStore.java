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
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.concurrent.TtlCache;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationCas;
import me.predatorray.candybox.coordination.CoordinationService;

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
    private final TtlCache<String, Optional<BoxAcl>> cache;

    BoxAclStore(CoordinationService coordination, Clock clock) {
        this.coordination = coordination;
        this.cache = new TtlCache<>(clock, CACHE_TTL_MILLIS);
    }

    /** The Box's ACL document, or empty when none was ever written (legacy Box). */
    Optional<BoxAcl> get(String box) {
        return cache.get(box, b -> coordination.get(CandyboxKeys.boxAclKey(b))
                .map(v -> BoxAcl.fromBytes(v.value())));
    }

    /** Creates or replaces the Box's ACL document (CAS with a short retry on races). */
    void set(String box, BoxAcl acl) {
        try {
            CoordinationCas.upsert(coordination, CandyboxKeys.boxAclKey(box), acl.toBytes(),
                    CAS_RETRIES);
        } catch (CasConflictException raced) {
            throw new CandyboxException("Concurrent ACL update on Box " + box, raced);
        }
        cache.invalidate(box);
    }

    /** Writes the initial ACL for a fresh Box; loses quietly if one already exists. */
    void seed(String box, BoxAcl acl) {
        try {
            coordination.create(CandyboxKeys.boxAclKey(box), acl.toBytes());
            cache.invalidate(box);
        } catch (CasConflictException alreadySeeded) {
            // a concurrent creator won; their ACL stands
        }
    }

    /** Drops the Box's ACL document when the Box is deleted; absent is fine. */
    void delete(String box) {
        try {
            CoordinationCas.deleteIfPresent(coordination, CandyboxKeys.boxAclKey(box), CAS_RETRIES);
        } catch (CasConflictException raced) {
            return; // someone else is mutating it concurrently with the delete; let it be
        }
        cache.invalidate(box);
    }
}
