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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.auth.Grant;
import me.predatorray.candybox.common.auth.Operation;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.auth.S3Key;
import me.predatorray.candybox.common.auth.S3KeyStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class S3AclTranslationTest {

    private static final long NOW_MILLIS = 1_750_000_000_000L; // fixed test clock
    private static final S3Key ALICE_KEY =
            new S3Key("AKIAALICE", "alice-secret", Principal.user("alice"));
    private static final S3Key BOB_KEY = new S3Key("AKIABOB", "bob-secret", Principal.user("bob"));

    private final FakeCandyStore store = new FakeCandyStore();
    private final ManualClock clock = new ManualClock(NOW_MILLIS);
    private final S3KeyStore keys = accessKeyId -> switch (accessKeyId) {
        case "AKIAALICE" -> Optional.of(ALICE_KEY);
        case "AKIABOB" -> Optional.of(BOB_KEY);
        default -> Optional.empty();
    };

    private static S3GatewayConfig config() {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "unused:2181");
        return S3GatewayConfig.fromProperties(props, Map.of());
    }

    private EmbeddedChannel channel(boolean allowAnonymous) {
        S3GatewayConfig config = config();
        S3Authenticator authenticator =
                new S3Authenticator(true, allowAnonymous, keys, "us-east-1", clock);
        return new EmbeddedChannel(
                new S3Handler(store, config, authenticator, new S3AccessControl(true, store)));
    }

    // ---- a minimal SigV4 request signer (header form) ---------------------------------------

    private Response exchange(EmbeddedChannel ch, HttpMethod method, String uri, byte[] body,
                              S3Key signAs, Map<String, String> extraHeaders) {
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method,
                uri, body == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(body));
        request.headers().set("Host", "127.0.0.1:9711");
        extraHeaders.forEach((k, v) -> request.headers().set(k, v));
        if (signAs != null) {
            sign(request, method.name(), uri, body, signAs);
        }
        ch.writeInbound(request);
        FullHttpResponse response = ch.readOutbound();
        String responseBody = response.content().toString(StandardCharsets.UTF_8);
        int status = response.status().code();
        response.release();
        return new Response(status, responseBody);
    }

    private record Response(int status, String body) {
    }

    private void sign(DefaultFullHttpRequest request, String method, String uri, byte[] body,
                      S3Key key) {
        String amzDate = SigV4.AMZ_DATE.format(Instant.ofEpochMilli(NOW_MILLIS));
        String payloadHash = SigV4.hex(SigV4.sha256(body == null ? new byte[0] : body));
        request.headers().set("x-amz-date", amzDate);
        request.headers().set("x-amz-content-sha256", payloadHash);

        Map<String, String> signedHeaderValues = new LinkedHashMap<>();
        signedHeaderValues.put("host", request.headers().get("Host"));
        signedHeaderValues.put("x-amz-content-sha256", payloadHash);
        signedHeaderValues.put("x-amz-date", amzDate);
        List<String> signedHeaders = List.of("host", "x-amz-content-sha256", "x-amz-date");

        int q = uri.indexOf('?');
        String rawPath = q < 0 ? uri : uri.substring(0, q);
        SigV4.Credential credential = new SigV4.Credential(key.accessKeyId(),
                amzDate.substring(0, 8), "us-east-1", "s3");
        String canonicalHash = SigV4.canonicalRequestHash(method, rawPath,
                SigV4.rawQueryParams(uri), signedHeaderValues, signedHeaders, payloadHash);
        String signature = SigV4.signature(SigV4.signingKey(key.secretKey(), credential),
                SigV4.stringToSign(amzDate, credential.scope(), canonicalHash));
        request.headers().set("Authorization", SigV4.ALGORITHM
                + " Credential=" + key.accessKeyId() + "/" + credential.scope()
                + ", SignedHeaders=" + String.join(";", signedHeaders)
                + ", Signature=" + signature);
    }

    private Response request(EmbeddedChannel ch, HttpMethod method, String uri,
                             String body, S3Key as, Map<String, String> headers) {
        return exchange(ch, method, uri,
                body == null ? null : body.getBytes(StandardCharsets.UTF_8), as, headers);
    }

    private void createObject(EmbeddedChannel ch) {
        assertThat(request(ch, HttpMethod.PUT, "/bucket", null, ALICE_KEY, Map.of()).status())
                .isEqualTo(200);
        assertThat(request(ch, HttpMethod.PUT, "/bucket/object", "value", ALICE_KEY,
                Map.of()).status()).isEqualTo(200);
    }

    private static String policy(String grantee, String permission) {
        return "<AccessControlPolicy><AccessControlList><Grant><Grantee>" + grantee
                + "</Grantee><Permission>" + permission
                + "</Permission></Grant></AccessControlList></AccessControlPolicy>";
    }

    private static List<S3RequestXml.AclGrant> renderedGrants(Response response) {
        assertThat(response.status()).isEqualTo(200);
        return S3RequestXml.parseAccessControlPolicy(
                response.body().getBytes(StandardCharsets.UTF_8)).grants();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "private", "bucket-owner-read", "bucket-owner-full-control"})
    void privateAndBucketOwnerCannedAclsHaveNoAdditionalGrants(String canned) {
        assertThat(S3AccessControl.cannedGrants(canned)).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"public-read,AllUsers,READ", "public-read-write,AllUsers,READ+WRITE",
            "authenticated-read,AuthenticatedUsers,READ"})
    void cannedAclsRoundTripThroughBucketHandler(String canned, String grantee, String operations) {
        EmbeddedChannel ch = channel(true);
        try {
            createObject(ch);
            assertThat(request(ch, HttpMethod.PUT, "/bucket?acl", null, ALICE_KEY,
                    Map.of("x-amz-acl", canned)).status()).isEqualTo(200);
            Grant expected = Grant.parse(grantee + ":" + operations);
            assertThat(store.getBoxAcl("bucket").orElseThrow().grants()).containsExactly(expected);
            Response response = request(ch, HttpMethod.GET, "/bucket?acl", null, ALICE_KEY, Map.of());
            String uri = grantee.equals(Grant.ALL_USERS) ? S3AccessControl.ALL_USERS_URI
                    : S3AccessControl.AUTHENTICATED_USERS_URI;
            List<S3RequestXml.AclGrant> rows = renderedGrants(response);
            assertThat(rows).contains(new S3RequestXml.AclGrant(null, "User:alice", "FULL_CONTROL"));
            assertThat(rows.stream().filter(g -> uri.equals(g.groupUri())).map(
                    S3RequestXml.AclGrant::permission)).containsExactly(operations.split("\\+"));
            assertThat(response.body()).contains("xsi:type=\"Group\"", "<URI>" + uri + "</URI>");
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"private", "bucket-owner-read", "bucket-owner-full-control"})
    void privateCannedAclsReplacePublicGrants(String canned) {
        EmbeddedChannel ch = channel(true);
        try {
            createObject(ch);
            assertThat(request(ch, HttpMethod.PUT, "/bucket?acl", null, ALICE_KEY,
                    Map.of("x-amz-acl", "public-read")).status()).isEqualTo(200);
            assertThat(request(ch, HttpMethod.PUT, "/bucket?acl", null, ALICE_KEY,
                    Map.of("x-amz-acl", canned)).status()).isEqualTo(200);
            assertThat(renderedGrants(request(ch, HttpMethod.GET, "/bucket?acl", null,
                    ALICE_KEY, Map.of()))).containsExactly(
                    new S3RequestXml.AclGrant(null, "User:alice", "FULL_CONTROL"));
            assertThat(store.getBoxAcl("bucket").orElseThrow().grants()).isEmpty();
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    void authenticatedReadObjectDistinguishesAnonymousFromOtherValidUser() {
        EmbeddedChannel ch = channel(true);
        try {
            createObject(ch);
            assertThat(request(ch, HttpMethod.PUT, "/bucket/object?acl", null, ALICE_KEY,
                    Map.of("x-amz-acl", "authenticated-read")).status()).isEqualTo(200);
            assertThat(request(ch, HttpMethod.GET, "/bucket/object", null, null, Map.of()).status())
                    .isEqualTo(403);
            Response bob = request(ch, HttpMethod.GET, "/bucket/object", null, BOB_KEY, Map.of());
            assertThat(bob.status()).isEqualTo(200);
            assertThat(bob.body()).isEqualTo("value");
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    void publicReadWriteBucketAllowsAnonymousObjectWriteButNotAclChanges() {
        EmbeddedChannel ch = channel(true);
        try {
            createObject(ch);
            assertThat(request(ch, HttpMethod.PUT, "/bucket?acl", null, ALICE_KEY,
                    Map.of("x-amz-acl", "public-read-write")).status()).isEqualTo(200);
            assertThat(request(ch, HttpMethod.PUT, "/bucket/anonymous", "public", null,
                    Map.of()).status()).isEqualTo(200);
            assertThat(request(ch, HttpMethod.GET, "/bucket/anonymous", null, null,
                    Map.of()).body()).isEqualTo("public");
            assertThat(request(ch, HttpMethod.GET, "/bucket?acl", null, null, Map.of()).status())
                    .isEqualTo(403);
            assertThat(request(ch, HttpMethod.PUT, "/bucket?acl", null, null, Map.of()).status())
                    .isEqualTo(403);
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL"})
    void canonicalUserPermissionsRoundTrip(String permission) {
        EmbeddedChannel ch = channel(true);
        try {
            createObject(ch);
            assertThat(request(ch, HttpMethod.PUT, "/bucket/object?acl",
                    policy("<ID>User:bob</ID>", permission), ALICE_KEY, Map.of()).status())
                    .isEqualTo(200);
            EnumSet<Operation> expected = permission.equals("FULL_CONTROL")
                    ? EnumSet.allOf(Operation.class) : EnumSet.of(Operation.valueOf(permission));
            assertThat(store.getCandyAcl("bucket", "object").grants()).containsExactly(
                    new Grant("User:bob", expected));
            assertThat(S3AccessControl.s3Permission(permission)).isEqualTo(expected);
            assertThat(renderedGrants(request(ch, HttpMethod.GET, "/bucket/object?acl", null,
                    ALICE_KEY, Map.of()))).containsExactly(
                    new S3RequestXml.AclGrant(null, "User:alice", "FULL_CONTROL"),
                    new S3RequestXml.AclGrant(null, "User:bob", permission));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"/bucket?acl", "/bucket/object?acl"})
    void readAcpAloneAllowsAclReadButDeniesObjectReadAndAclWrite(String target) {
        EmbeddedChannel ch = channel(true);
        try {
            createObject(ch);
            assertThat(request(ch, HttpMethod.PUT, target,
                    policy("<ID>User:bob</ID>", "READ_ACP"), ALICE_KEY, Map.of()).status())
                    .isEqualTo(200);
            assertThat(request(ch, HttpMethod.GET, target, null, BOB_KEY, Map.of()).status())
                    .isEqualTo(200);
            assertThat(request(ch, HttpMethod.GET, "/bucket/object", null, BOB_KEY, Map.of()).status())
                    .isEqualTo(403);
            assertThat(request(ch, HttpMethod.PUT, target, null, BOB_KEY, Map.of()).status())
                    .isEqualTo(403);
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    void writeAcpAllowsBucketAclChangeWithoutReadOrAdmin() {
        EmbeddedChannel ch = channel(true);
        try {
            createObject(ch);
            assertThat(request(ch, HttpMethod.PUT, "/bucket?acl",
                    policy("<ID>User:bob</ID>", "WRITE_ACP"), ALICE_KEY, Map.of()).status())
                    .isEqualTo(200);
            assertThat(request(ch, HttpMethod.GET, "/bucket?acl", null, BOB_KEY, Map.of()).status())
                    .isEqualTo(403);
            assertThat(request(ch, HttpMethod.DELETE, "/bucket", null, BOB_KEY, Map.of()).status())
                    .isEqualTo(403);
            assertThat(request(ch, HttpMethod.PUT, "/bucket?acl", null, BOB_KEY, Map.of()).status())
                    .isEqualTo(200);
            assertThat(store.getBoxAcl("bucket").orElseThrow().owner()).isEqualTo(ALICE_KEY.principal());
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {S3AccessControl.ALL_USERS_URI, S3AccessControl.AUTHENTICATED_USERS_URI})
    void xmlGroupsRoundTrip(String uri) {
        EmbeddedChannel ch = channel(true);
        try {
            createObject(ch);
            assertThat(request(ch, HttpMethod.PUT, "/bucket?acl",
                    policy("<URI>" + uri + "</URI>", "READ"), ALICE_KEY, Map.of()).status())
                    .isEqualTo(200);
            String grantee = uri.equals(S3AccessControl.ALL_USERS_URI)
                    ? Grant.ALL_USERS : Grant.AUTHENTICATED_USERS;
            assertThat(store.getBoxAcl("bucket").orElseThrow().grants())
                    .containsExactly(Grant.of(grantee, Operation.READ));
            assertThat(renderedGrants(request(ch, HttpMethod.GET, "/bucket?acl", null,
                    ALICE_KEY, Map.of()))).contains(new S3RequestXml.AclGrant(uri, null, "READ"));
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    void adminIsNeverRenderedAsAnS3Permission() {
        assertThat(S3AccessControl.s3PermissionNames(Grant.of("User:bob", Operation.ADMIN)))
                .isEmpty();
        assertThat(S3AccessControl.s3PermissionNames(Grant.of("User:bob",
                Operation.ADMIN, Operation.READ, Operation.WRITE_ACP)))
                .containsExactly("READ", "WRITE_ACP");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/bucket?acl", "/bucket/object?acl"})
    void emptyBodyClearsExistingGrantsWhilePreservingOwner(String target) {
        EmbeddedChannel ch = channel(true);
        try {
            createObject(ch);
            assertThat(request(ch, HttpMethod.PUT, target, null, ALICE_KEY,
                    Map.of("x-amz-acl", "public-read")).status()).isEqualTo(200);
            assertThat(request(ch, HttpMethod.PUT, target, "", ALICE_KEY, Map.of()).status())
                    .isEqualTo(200);
            assertThat(renderedGrants(request(ch, HttpMethod.GET, target, null, ALICE_KEY, Map.of())))
                    .containsExactly(new S3RequestXml.AclGrant(null, "User:alice", "FULL_CONTROL"));
            assertThat(request(ch, HttpMethod.GET, "/bucket/object", null, null, Map.of()).status())
                    .isEqualTo(403);
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "PUT"})
    void missingBucketAclAnswersNoSuchBucket(String method) {
        EmbeddedChannel ch = new EmbeddedChannel(new S3Handler(store, config()));
        try {
            Response response = request(ch, HttpMethod.valueOf(method), "/absent?acl", null,
                    null, Map.of());
            assertThat(response.status()).isEqualTo(404);
            assertThat(response.body()).contains("<Code>NoSuchBucket</Code>");
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    void anonymousCannotAssignOwnerToLegacyBucket() {
        store.createBox("legacy");
        EmbeddedChannel ch = new EmbeddedChannel(new S3Handler(store, config()));
        try {
            Response response = request(ch, HttpMethod.PUT, "/legacy?acl", null, null,
                    Map.of("x-amz-acl", "public-read"));
            assertThat(response.status()).isEqualTo(403);
            assertThat(response.body()).contains("<Code>AccessDenied</Code>");
            assertThat(store.getBoxAcl("legacy")).isEmpty();
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    void invalidMappingsHaveSpecificS3Errors() {
        assertError(() -> S3AccessControl.cannedGrants("unknown"), S3ErrorCode.INVALID_ARGUMENT);
        assertError(() -> S3AccessControl.s3Permission("ADMIN"), S3ErrorCode.MALFORMED_XML);
        assertError(() -> S3AccessControl.granteeFromS3("https://unknown", null),
                S3ErrorCode.MALFORMED_XML);
        assertError(() -> S3AccessControl.granteeFromS3(null, null), S3ErrorCode.MALFORMED_XML);
        assertError(() -> S3AccessControl.granteeFromS3(null, " "), S3ErrorCode.MALFORMED_XML);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "<AccessControlPolicy><Grant><Grantee><ID>User:bob</ID></Grantee></Grant></AccessControlPolicy>",
            "<AccessControlPolicy><Grant><Permission>READ</Permission></Grant></AccessControlPolicy>",
            "<AccessControlPolicy><Grant>",
            "not XML"})
    void malformedPolicyAnswersMalformedXml(String xml) {
        assertError(() -> S3RequestXml.parseAccessControlPolicy(xml.getBytes(StandardCharsets.UTF_8)),
                S3ErrorCode.MALFORMED_XML);
    }

    @ParameterizedTest
    @CsvSource({"DELETE_BUCKET,ADMIN", "GET_BUCKET_ACL,READ_ACP", "PUT_BUCKET_ACL,WRITE_ACP",
            "GET_OBJECT,READ", "HEAD_OBJECT,READ", "GET_OBJECT_ACL,READ_ACP",
            "PUT_OBJECT_ACL,WRITE_ACP", "HEAD_BUCKET,READ", "LIST_OBJECTS,READ",
            "LIST_OBJECT_VERSIONS,READ", "GET_BUCKET_LOCATION,READ", "GET_BUCKET_VERSIONING,READ",
            "LIST_MULTIPART_UPLOADS,READ", "LIST_PARTS,READ", "PUT_OBJECT,WRITE",
            "DELETE_OBJECT,WRITE", "DELETE_OBJECTS,WRITE", "CREATE_MULTIPART_UPLOAD,WRITE",
            "UPLOAD_PART,WRITE", "COMPLETE_MULTIPART_UPLOAD,WRITE", "ABORT_MULTIPART_UPLOAD,WRITE"})
    void routedActionsRequireTheirSpecificOperation(S3Router.S3Action action, Operation required) {
        store.createBox("bucket");
        S3AccessControl access = new S3AccessControl(true, store);
        S3Handler.PathParts parts = new S3Handler.PathParts("bucket", "object");
        for (Operation granted : Operation.values()) {
            store.setBoxAcl("bucket", new BoxAcl(ALICE_KEY.principal(),
                    List.of(Grant.of("User:bob", granted))));
            access.invalidate("bucket");
            if (granted == required) {
                access.authorize(action, parts, BOB_KEY.principal());
            } else {
                assertError(() -> access.authorize(action, parts, BOB_KEY.principal()),
                        S3ErrorCode.ACCESS_DENIED);
            }
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    void objectAclUnionDoesNotAcceptMissingKey(String key) {
        store.setBoxAcl("bucket", new BoxAcl(ALICE_KEY.principal(), List.of()));
        S3AccessControl access = new S3AccessControl(true, store);
        assertError(() -> access.authorize(S3Router.S3Action.GET_OBJECT_ACL,
                new S3Handler.PathParts("bucket", key), BOB_KEY.principal()), S3ErrorCode.ACCESS_DENIED);
    }

    @Test
    void disabledAccessAndOwnerVisibilityFollowAclState() {
        S3AccessControl disabled = new S3AccessControl(false, store);
        assertThat(disabled.owns("absent", BOB_KEY.principal())).isTrue();
        S3AccessControl enabled = new S3AccessControl(true, store);
        assertThat(enabled.owns("absent", BOB_KEY.principal())).isFalse();
        store.setBoxAcl("bucket", new BoxAcl(ALICE_KEY.principal(),
                List.of(Grant.of("User:bob", Operation.READ))));
        assertThat(enabled.owns("bucket", ALICE_KEY.principal())).isTrue();
        assertThat(enabled.owns("bucket", BOB_KEY.principal())).isFalse();
        // Unsupported routes must reach the handler's 501 response without an ACL decision.
        enabled.authorize(S3Router.S3Action.UNSUPPORTED,
                new S3Handler.PathParts("bucket", null), Principal.ANONYMOUS);
    }

    private static void assertError(Runnable action, S3ErrorCode error) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(S3Exception.class,
                e -> assertThat(e.error()).isEqualTo(error));
    }
}
