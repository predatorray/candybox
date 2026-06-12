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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.auth.Grant;
import me.predatorray.candybox.common.auth.Operation;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.auth.StandardAuthorizer;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.ConnectionContext;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.junit.jupiter.api.Test;

/**
 * Authorization end-to-end through {@link NodeRequestHandler}: per-Box ACLs (owner, grants, the
 * AllUsers/AuthenticatedUsers groups), the cluster policy, Box-list filtering, the ACL management
 * opcodes, and the {@code RESPONSE_ACCESS_DENIED} mapping — all against the in-memory fakes.
 */
class AuthorizationTest {

    private static final MessageCodec CODEC = new MessageCodec();
    private static final Principal ALICE = Principal.user("alice");
    private static final Principal BOB = Principal.user("bob");

    private static CandyboxConfig config() {
        return CandyboxConfig.builder().multipartMinPartBytes(0).build();
    }

    private static CandyboxNode newAuthorizedNode() {
        CandyboxNode node = new CandyboxNode(1, config(), new InMemoryLedgerStore(),
                new InMemoryCoordinationService(), new ManualClock(1000));
        node.authorizer(new StandardAuthorizer(List.of("Admin:ops"),
                box -> node.aclStore().get(box)));
        return node;
    }

    private static Message call(RequestHandler handler, Principal principal, Message request) {
        ConnectionContext context = new ConnectionContext();
        if (principal != null) {
            context.authenticated(principal);
        }
        Frame response = handler.handle(context, CODEC.encode(request));
        return CODEC.decode(response);
    }

    private static Message.PutCandyRequest put(String box, String key) {
        return new Message.PutCandyRequest(box, key, null, null, null,
                "v".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void creatorOwnsTheBoxAndOthersAreDeniedByDefault() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            assertThat(call(handler, ALICE, new Message.CreateBoxRequest("photos", 1)))
                    .isInstanceOf(Message.OkResponse.class);
            // The creator can write and read ...
            assertThat(call(handler, ALICE, put("photos", "k")))
                    .isInstanceOf(Message.OkResponse.class);
            assertThat(call(handler, ALICE, new Message.GetCandyRequest("photos", "k")))
                    .isInstanceOf(Message.CandyDataResponse.class);
            // ... another authenticated user is denied, and anonymous too.
            assertThat(call(handler, BOB, new Message.GetCandyRequest("photos", "k")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
            assertThat(call(handler, null, new Message.GetCandyRequest("photos", "k")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
        }
    }

    @Test
    void grantsOpenTheBoxSelectively() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("shared", 1));
            call(handler, ALICE, put("shared", "k"));

            // Grant bob READ; he can read but not write.
            assertThat(call(handler, ALICE, new Message.SetBoxAclRequest("shared", "User:alice",
                    List.of("User:bob:READ")))).isInstanceOf(Message.OkResponse.class);
            assertThat(call(handler, BOB, new Message.GetCandyRequest("shared", "k")))
                    .isInstanceOf(Message.CandyDataResponse.class);
            assertThat(call(handler, BOB, put("shared", "k2")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);

            // AllUsers:READ opens reads to anonymous as well (the S3 public-read shape).
            call(handler, ALICE, new Message.SetBoxAclRequest("shared", "User:alice",
                    List.of(Grant.of(Grant.ALL_USERS, Operation.READ).toText())));
            assertThat(call(handler, null, new Message.GetCandyRequest("shared", "k")))
                    .isInstanceOf(Message.CandyDataResponse.class);
            assertThat(call(handler, null, put("shared", "k3")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
        }
    }

    @Test
    void onlyPrincipalsWithReadSeeTheBoxInListings() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("alices", 1));
            call(handler, BOB, new Message.CreateBoxRequest("bobs", 1));

            Message.ListBoxesResponse aliceSees = (Message.ListBoxesResponse)
                    call(handler, ALICE, new Message.ListBoxesRequest());
            assertThat(aliceSees.boxes()).containsExactly("alices");
            Message.ListBoxesResponse bobSees = (Message.ListBoxesResponse)
                    call(handler, BOB, new Message.ListBoxesRequest());
            assertThat(bobSees.boxes()).containsExactly("bobs");
        }
    }

    @Test
    void anonymousMayNotCreateBoxesOrListWhenAuthorized() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            assertThat(call(handler, null, new Message.CreateBoxRequest("nope", 1)))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
            assertThat(call(handler, null, new Message.ListBoxesRequest()))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
        }
    }

    @Test
    void deleteBoxNeedsAdminWhichGrantsCanConfer() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("doomed", 1));
            assertThat(call(handler, BOB, new Message.DeleteBoxRequest("doomed", false)))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
            call(handler, ALICE, new Message.SetBoxAclRequest("doomed", "User:alice",
                    List.of("User:bob:ADMIN")));
            assertThat(call(handler, BOB, new Message.DeleteBoxRequest("doomed", false)))
                    .isInstanceOf(Message.OkResponse.class);
        }
    }

    @Test
    void aclOpsAreThemselvesAuthorized() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("locked", 1));
            // bob cannot read or replace the ACL of a Box he has no grants on.
            assertThat(call(handler, BOB, new Message.GetBoxAclRequest("locked")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
            assertThat(call(handler, BOB, new Message.SetBoxAclRequest("locked", "User:bob",
                    List.of()))).isInstanceOf(Message.AccessDeniedResponse.class);
            // the owner reads it back.
            Message.BoxAclResponse acl = (Message.BoxAclResponse)
                    call(handler, ALICE, new Message.GetBoxAclRequest("locked"));
            assertThat(acl.owner()).isEqualTo("User:alice");
            assertThat(acl.grants()).isEmpty();
        }
    }

    @Test
    void superUsersBypassAclsEntirely() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("private", 1));
            Principal ops = new Principal(Principal.TYPE_ADMIN, "ops");
            assertThat(call(handler, ops, put("private", "k")))
                    .isInstanceOf(Message.OkResponse.class);
            assertThat(call(handler, ops, new Message.DeleteBoxRequest("private", true)))
                    .isInstanceOf(Message.OkResponse.class);
        }
    }

    @Test
    void deletingABoxDropsItsAclDocument() {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        try (CandyboxNode node = new CandyboxNode(1, config(), new InMemoryLedgerStore(),
                coordination, new ManualClock(1000))) {
            node.authorizer(new StandardAuthorizer(List.of(), box -> node.aclStore().get(box)));
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("gone", 1));
            assertThat(coordination.get("acls/gone")).isPresent();
            call(handler, ALICE, new Message.DeleteBoxRequest("gone", true));
            assertThat(coordination.get("acls/gone")).isEmpty();
        }
    }

    @Test
    void writesStampTheConnectionPrincipalAsObjectOwner() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("owned", 1));
            call(handler, ALICE, put("owned", "k"));
            Message.CandyAclResponse acl = (Message.CandyAclResponse)
                    call(handler, ALICE, new Message.GetCandyAclRequest("owned", "k"));
            assertThat(acl.owner()).isEqualTo("User:alice");
            assertThat(acl.grants()).isEmpty();
        }
    }

    @Test
    void objectGrantsOpenSingleObjectsInAPrivateBox() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("mostly-private", 1));
            call(handler, ALICE, put("mostly-private", "secret"));
            // A PUT carrying grants — the S3 `x-amz-acl: public-read` shape.
            call(handler, ALICE, new Message.PutCandyRequest("mostly-private", "public", null,
                    null, null, "v".getBytes(StandardCharsets.UTF_8), null,
                    List.of("AllUsers:READ")));

            // The granted object is readable by bob and by anonymous; the other is not.
            assertThat(call(handler, BOB, new Message.GetCandyRequest("mostly-private", "public")))
                    .isInstanceOf(Message.CandyDataResponse.class);
            assertThat(call(handler, null, new Message.GetCandyRequest("mostly-private", "public")))
                    .isInstanceOf(Message.CandyDataResponse.class);
            assertThat(call(handler, BOB, new Message.GetCandyRequest("mostly-private", "secret")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
            // Object grants never open writes (bucket WRITE governs).
            assertThat(call(handler, BOB, put("mostly-private", "public")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
            // And a missing object resolves to AccessDenied, not NotFound (no existence leak).
            assertThat(call(handler, BOB, new Message.GetCandyRequest("mostly-private", "nope")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
        }
    }

    @Test
    void setCandyAclRewritesGrantsWithoutMovingBytes() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("flip", 1));
            call(handler, ALICE, put("flip", "k"));
            assertThat(call(handler, BOB, new Message.GetCandyRequest("flip", "k")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);

            assertThat(call(handler, ALICE, new Message.SetCandyAclRequest("flip", "k",
                    "User:alice", List.of("AllUsers:READ"))))
                    .isInstanceOf(Message.OkResponse.class);
            Message.CandyDataResponse data = (Message.CandyDataResponse)
                    call(handler, BOB, new Message.GetCandyRequest("flip", "k"));
            assertThat(data.data()).isEqualTo("v".getBytes(StandardCharsets.UTF_8));

            // Revoking flips it back (the locator rewrite is LWW-newer).
            call(handler, ALICE, new Message.SetCandyAclRequest("flip", "k", "User:alice",
                    List.of()));
            assertThat(call(handler, BOB, new Message.GetCandyRequest("flip", "k")))
                    .isInstanceOf(Message.AccessDeniedResponse.class);
        }
    }

    @Test
    void objectOwnerCanManageItsAclEvenWithoutBoxAcp() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("shared-writes", 1));
            call(handler, ALICE, new Message.SetBoxAclRequest("shared-writes", "User:alice",
                    List.of("User:bob:READ+WRITE")));
            // bob writes his own object; he owns it, so he can read/replace its ACL even though
            // the Box grants him no READ_ACP/WRITE_ACP.
            call(handler, BOB, put("shared-writes", "bobs"));
            Message.CandyAclResponse acl = (Message.CandyAclResponse)
                    call(handler, BOB, new Message.GetCandyAclRequest("shared-writes", "bobs"));
            assertThat(acl.owner()).isEqualTo("User:bob");
            assertThat(call(handler, BOB, new Message.SetCandyAclRequest("shared-writes", "bobs",
                    "User:bob", List.of("AllUsers:READ")))).isInstanceOf(Message.OkResponse.class);
        }
    }

    @Test
    void copyBelongsToTheRequesterWhileRenameKeepsTheAcl() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            call(handler, ALICE, new Message.CreateBoxRequest("movers", 1));
            call(handler, ALICE, new Message.SetBoxAclRequest("movers", "User:alice",
                    List.of("User:bob:READ+WRITE")));
            call(handler, ALICE, new Message.PutCandyRequest("movers", "src", null, null, null,
                    "v".getBytes(StandardCharsets.UTF_8), null, List.of("AllUsers:READ")));

            // bob's copy: owner=bob, source grants NOT inherited (S3 CopyObject semantics).
            call(handler, BOB, new Message.CopyCandyRequest("movers", "src", "bobs-copy", null));
            Message.CandyAclResponse copyAcl = (Message.CandyAclResponse)
                    call(handler, BOB, new Message.GetCandyAclRequest("movers", "bobs-copy"));
            assertThat(copyAcl.owner()).isEqualTo("User:bob");
            assertThat(copyAcl.grants()).isEmpty();

            // alice's rename: the object moves with its owner + grants intact.
            call(handler, ALICE, new Message.RenameCandyRequest("movers", "src", "moved", null));
            Message.CandyAclResponse movedAcl = (Message.CandyAclResponse)
                    call(handler, ALICE, new Message.GetCandyAclRequest("movers", "moved"));
            assertThat(movedAcl.owner()).isEqualTo("User:alice");
            assertThat(movedAcl.grants()).hasSize(1);
        }
    }

    @Test
    void ownerOverrideIsOnlyHonoredFromSuperPrincipals() {
        try (CandyboxNode node = newAuthorizedNode()) {
            RequestHandler handler = node.requestHandler();
            Principal gateway = new Principal(Principal.TYPE_ADMIN, "ops"); // super in this setup
            call(handler, ALICE, new Message.CreateBoxRequest("fronted", 1));

            // The super-principal writes on behalf of bob: bob owns the object.
            call(handler, gateway, new Message.PutCandyRequest("fronted", "via-gw", null, null,
                    null, "v".getBytes(StandardCharsets.UTF_8), "User:bob", List.of()));
            Message.CandyAclResponse viaGw = (Message.CandyAclResponse)
                    call(handler, ALICE, new Message.GetCandyAclRequest("fronted", "via-gw"));
            assertThat(viaGw.owner()).isEqualTo("User:bob");

            // A regular user claiming someone else's identity is ignored: they own it themselves.
            call(handler, ALICE, new Message.PutCandyRequest("fronted", "spoofed", null, null,
                    null, "v".getBytes(StandardCharsets.UTF_8), "User:bob", List.of()));
            Message.CandyAclResponse spoofed = (Message.CandyAclResponse)
                    call(handler, ALICE, new Message.GetCandyAclRequest("fronted", "spoofed"));
            assertThat(spoofed.owner()).isEqualTo("User:alice");
        }
    }

    @Test
    void withoutAnAuthorizerEverythingStaysOpen() {
        try (CandyboxNode node = new CandyboxNode(1, config(), new InMemoryLedgerStore(),
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            RequestHandler handler = node.requestHandler();
            assertThat(call(handler, null, new Message.CreateBoxRequest("open", 1)))
                    .isInstanceOf(Message.OkResponse.class);
            assertThat(call(handler, null, put("open", "k")))
                    .isInstanceOf(Message.OkResponse.class);
        }
    }
}
