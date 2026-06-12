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
package me.predatorray.candybox.common.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StandardAuthorizerTest {

    private static final Principal ALICE = Principal.user("alice");
    private static final Principal BOB = Principal.user("bob");
    private static final Principal GATEWAY = new Principal(Principal.TYPE_GATEWAY, "s3-gw");
    private static final Principal NODE = new Principal(Principal.TYPE_NODE, "3");

    private final Map<String, BoxAcl> acls = Map.of(
            "private-box", BoxAcl.privateTo(ALICE),
            "public-read", new BoxAcl(ALICE, List.of(Grant.of(Grant.ALL_USERS, Operation.READ))),
            "team-box", new BoxAcl(ALICE, List.of(
                    Grant.of("User:bob", Operation.READ, Operation.WRITE),
                    Grant.of(Grant.AUTHENTICATED_USERS, Operation.READ))));

    private final StandardAuthorizer authorizer = new StandardAuthorizer(
            List.of("Gateway:s3-gw", "Node:*"),
            box -> Optional.ofNullable(acls.get(box)));

    @Test
    void ownerHasEveryOperation() {
        for (Operation op : Operation.values()) {
            assertTrue(authorizer.authorize(ALICE, op, Resource.box("private-box")));
        }
    }

    @Test
    void nonOwnerGetsOnlyTheGrantedOperations() {
        assertTrue(authorizer.authorize(BOB, Operation.READ, Resource.box("team-box")));
        assertTrue(authorizer.authorize(BOB, Operation.WRITE, Resource.box("team-box")));
        assertFalse(authorizer.authorize(BOB, Operation.ADMIN, Resource.box("team-box")));
        assertFalse(authorizer.authorize(BOB, Operation.WRITE_ACP, Resource.box("team-box")));
        assertFalse(authorizer.authorize(BOB, Operation.READ, Resource.box("private-box")));
    }

    @Test
    void allUsersGrantIncludesAnonymous() {
        assertTrue(authorizer.authorize(Principal.ANONYMOUS, Operation.READ,
                Resource.box("public-read")));
        assertFalse(authorizer.authorize(Principal.ANONYMOUS, Operation.WRITE,
                Resource.box("public-read")));
    }

    @Test
    void authenticatedUsersGrantExcludesAnonymous() {
        assertTrue(authorizer.authorize(BOB, Operation.READ, Resource.box("team-box")));
        assertFalse(authorizer.authorize(Principal.ANONYMOUS, Operation.READ,
                Resource.box("team-box")));
    }

    @Test
    void superUsersBypassEverything() {
        assertTrue(authorizer.authorize(GATEWAY, Operation.ADMIN, Resource.box("private-box")));
        // Node:* is a type wildcard, matching any node principal.
        assertTrue(authorizer.authorize(NODE, Operation.WRITE, Resource.box("private-box")));
        assertTrue(authorizer.authorize(GATEWAY, Operation.WRITE, Resource.CLUSTER));
    }

    @Test
    void clusterPolicyIsAuthenticatedOnly() {
        assertTrue(authorizer.authorize(BOB, Operation.WRITE, Resource.CLUSTER));
        assertFalse(authorizer.authorize(Principal.ANONYMOUS, Operation.WRITE, Resource.CLUSTER));
    }

    @Test
    void boxWithoutAnAclFallsBackToAuthenticatedFullAccess() {
        assertTrue(authorizer.authorize(BOB, Operation.WRITE, Resource.box("legacy-box")));
        assertFalse(authorizer.authorize(Principal.ANONYMOUS, Operation.READ,
                Resource.box("legacy-box")));
    }

    @Test
    void aclDocumentRoundTripsThroughItsTextForm() {
        BoxAcl acl = new BoxAcl(ALICE, List.of(
                Grant.of(Grant.ALL_USERS, Operation.READ),
                Grant.of("User:bob", Operation.READ, Operation.WRITE, Operation.WRITE_ACP)));
        BoxAcl parsed = BoxAcl.fromBytes(acl.toBytes());
        assertEquals(acl.owner(), parsed.owner());
        assertEquals(acl.grants().size(), parsed.grants().size());
        assertTrue(parsed.permits(BOB, Operation.WRITE_ACP));
        assertTrue(parsed.permits(Principal.ANONYMOUS, Operation.READ));
        assertFalse(parsed.permits(Principal.ANONYMOUS, Operation.WRITE));
    }

    @Test
    void malformedAclDocumentsAreRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> BoxAcl.fromBytes("grant=AllUsers:READ\n".getBytes()));
        assertThrows(IllegalArgumentException.class,
                () -> BoxAcl.fromBytes("owner=User:a\nbogus line\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> Grant.parse("AllUsers"));
        assertThrows(IllegalArgumentException.class, () -> Grant.parse("AllUsers:NOT_AN_OP"));
    }
}
