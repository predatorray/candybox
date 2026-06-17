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
package me.predatorray.candybox.s3;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.auth.Grant;
import me.predatorray.candybox.common.auth.ObjectAcl;
import me.predatorray.candybox.common.auth.Operation;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.auth.Resource;
import me.predatorray.candybox.common.auth.StandardAuthorizer;
import me.predatorray.candybox.s3.S3Router.S3Action;

/**
 * The gateway's authorization: the same {@link StandardAuthorizer} + ACL documents the nodes use,
 * evaluated here against the authenticated S3 principal (the gateway itself talks to the cluster as
 * a super-principal, so end-user enforcement happens at this layer). Box ACLs are fetched through
 * the store and cached briefly; the S3 union rule consults the object's own grants for
 * single-object reads the Box ACL denies.
 *
 * <p>Also owns the S3↔Candybox ACL translation: canned ACLs ({@code x-amz-acl}), the
 * {@code AccessControlPolicy} XML rendering/parsing pairs, and the permission mapping
 * (S3 {@code FULL_CONTROL} = all five Candybox operations).
 */
final class S3AccessControl {

    private static final long CACHE_TTL_MILLIS = 5_000;

    static final String ALL_USERS_URI = "http://acs.amazonaws.com/groups/global/AllUsers";
    static final String AUTHENTICATED_USERS_URI =
            "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

    private final boolean enabled;
    private final StandardAuthorizer authorizer;
    private final CandyStore store;
    private final Map<String, CachedAcl> cache = new ConcurrentHashMap<>();

    private record CachedAcl(Optional<BoxAcl> acl, long expiresAtMillis) {
    }

    S3AccessControl(boolean enabled, CandyStore store) {
        this.enabled = enabled;
        this.store = store;
        this.authorizer = new StandardAuthorizer(List.of(), this::boxAcl);
    }

    boolean enabled() {
        return enabled;
    }

    private Optional<BoxAcl> boxAcl(String box) {
        long now = System.currentTimeMillis();
        CachedAcl cached = cache.get(box);
        if (cached != null && now < cached.expiresAtMillis) {
            return cached.acl;
        }
        Optional<BoxAcl> acl = store.getBoxAcl(box);
        cache.put(box, new CachedAcl(acl, now + CACHE_TTL_MILLIS));
        return acl;
    }

    void invalidate(String box) {
        cache.remove(box);
    }

    /** Authorizes one routed request; throws {@code AccessDenied} otherwise. */
    void authorize(S3Action action, S3Handler.PathParts parts, Principal principal) {
        if (!enabled) {
            return;
        }
        switch (action) {
            case LIST_BUCKETS -> {
                // Anonymous ListBuckets answers an empty list rather than 403 (RGW-compatible);
                // per-bucket visibility is filtered by the handler.
            }
            case CREATE_BUCKET -> require(principal, Operation.WRITE, Resource.CLUSTER, null, null);
            case DELETE_BUCKET ->
                    require(principal, Operation.ADMIN, Resource.box(parts.bucket()), null, null);
            case GET_BUCKET_ACL -> require(principal, Operation.READ_ACP,
                    Resource.box(parts.bucket()), null, null);
            case PUT_BUCKET_ACL -> require(principal, Operation.WRITE_ACP,
                    Resource.box(parts.bucket()), null, null);
            case GET_OBJECT, HEAD_OBJECT -> require(principal, Operation.READ,
                    Resource.box(parts.bucket()), parts.bucket(), parts.key());
            case GET_OBJECT_ACL -> require(principal, Operation.READ_ACP,
                    Resource.box(parts.bucket()), parts.bucket(), parts.key());
            case PUT_OBJECT_ACL -> require(principal, Operation.WRITE_ACP,
                    Resource.box(parts.bucket()), parts.bucket(), parts.key());
            case HEAD_BUCKET, LIST_OBJECTS, LIST_OBJECT_VERSIONS, GET_BUCKET_LOCATION,
                    GET_BUCKET_VERSIONING, LIST_MULTIPART_UPLOADS, LIST_PARTS ->
                    require(principal, Operation.READ, Resource.box(parts.bucket()), null, null);
            case PUT_OBJECT, DELETE_OBJECT, DELETE_OBJECTS, CREATE_MULTIPART_UPLOAD, UPLOAD_PART,
                    COMPLETE_MULTIPART_UPLOAD, ABORT_MULTIPART_UPLOAD ->
                    require(principal, Operation.WRITE, Resource.box(parts.bucket()), null, null);
            default -> {
                // UNSUPPORTED falls through to the 501 path without an authorization decision.
            }
        }
    }

    /** True when {@code principal} owns the Box. {@code ListAllMyBuckets} lists owned buckets only —
     * never buckets merely granted READ (e.g. a {@code public-read} bucket owned by someone else),
     * matching AWS/RGW, so one account's public bucket can't leak into another account's listing. */
    boolean owns(String box, Principal principal) {
        if (!enabled) {
            return true;
        }
        return boxAcl(box).map(acl -> acl.owner().equals(principal)).orElse(false);
    }

    private void require(Principal principal, Operation operation, Resource resource,
                         String unionBucket, String unionKey) {
        if (authorizer.authorize(principal, operation, resource)) {
            return;
        }
        // The S3 union rule: single-object READ/ACP requests may be opened by the object's own
        // grants. Resolution failures (no such object) deny — no existence leak.
        if (unionBucket != null && unionKey != null && !unionKey.isEmpty()) {
            try {
                ObjectAcl acl = store.getCandyAcl(unionBucket, unionKey);
                if (acl.permits(principal, operation)) {
                    return;
                }
            } catch (RuntimeException resolveFailed) {
                // fall through to the denial
            }
        }
        throw new S3Exception(S3ErrorCode.ACCESS_DENIED, "Access Denied");
    }

    // ---- S3 <-> Candybox ACL translation ------------------------------------------------------

    /** Maps an {@code x-amz-acl} canned ACL onto Candybox grants; null/empty header ⇒ private. */
    static List<Grant> cannedGrants(String cannedAcl) {
        if (cannedAcl == null || cannedAcl.isBlank()) {
            return List.of();
        }
        return switch (cannedAcl.trim().toLowerCase(Locale.ROOT)) {
            case "private" -> List.of();
            case "public-read" -> List.of(Grant.of(Grant.ALL_USERS, Operation.READ));
            case "public-read-write" ->
                    List.of(Grant.of(Grant.ALL_USERS, Operation.READ, Operation.WRITE));
            case "authenticated-read" ->
                    List.of(Grant.of(Grant.AUTHENTICATED_USERS, Operation.READ));
            case "bucket-owner-read", "bucket-owner-full-control" ->
                    // The Box owner already has full control in the Candybox model.
                    List.of();
            default -> throw new S3Exception(S3ErrorCode.INVALID_ARGUMENT,
                    "Unsupported canned ACL: " + cannedAcl);
        };
    }

    /** Maps one S3 permission name onto the Candybox operations it grants. */
    static EnumSet<Operation> s3Permission(String permission) {
        return switch (permission.trim().toUpperCase(Locale.ROOT)) {
            case "READ" -> EnumSet.of(Operation.READ);
            case "WRITE" -> EnumSet.of(Operation.WRITE);
            case "READ_ACP" -> EnumSet.of(Operation.READ_ACP);
            case "WRITE_ACP" -> EnumSet.of(Operation.WRITE_ACP);
            case "FULL_CONTROL" -> EnumSet.allOf(Operation.class);
            default -> throw new S3Exception(S3ErrorCode.MALFORMED_XML,
                    "Unknown ACL permission: " + permission);
        };
    }

    /** The reverse: which S3 permission names one Candybox grant renders as. */
    static List<String> s3PermissionNames(Grant grant) {
        if (grant.operations().containsAll(EnumSet.allOf(Operation.class))) {
            return List.of("FULL_CONTROL");
        }
        List<String> names = new ArrayList<>();
        for (Operation op : Operation.values()) {
            if (grant.operations().contains(op) && op != Operation.ADMIN) {
                names.add(op.name());
            }
        }
        return names;
    }

    /** An S3 grantee (group URI or canonical user id) → Candybox grantee string. */
    static String granteeFromS3(String groupUri, String canonicalId) {
        if (groupUri != null) {
            if (ALL_USERS_URI.equals(groupUri)) {
                return Grant.ALL_USERS;
            }
            if (AUTHENTICATED_USERS_URI.equals(groupUri)) {
                return Grant.AUTHENTICATED_USERS;
            }
            throw new S3Exception(S3ErrorCode.MALFORMED_XML, "Unknown grantee group: " + groupUri);
        }
        if (canonicalId == null || canonicalId.isBlank()) {
            throw new S3Exception(S3ErrorCode.MALFORMED_XML, "Grantee needs an ID or a group URI");
        }
        // Candybox principal strings ("User:alice") double as S3 canonical user ids.
        return Principal.parse(canonicalId).toString();
    }
}
