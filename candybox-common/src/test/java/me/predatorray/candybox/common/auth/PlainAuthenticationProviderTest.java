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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import me.predatorray.candybox.common.exception.AuthenticationException;
import org.junit.jupiter.api.Test;

class PlainAuthenticationProviderTest {

    private final PlainAuthenticationProvider provider = new PlainAuthenticationProvider();
    private final InMemoryCredentialStore store = new InMemoryCredentialStore()
            .addUser("alice", "wonderland")
            .addUser("s3-gw", "gateway-pw", new Principal(Principal.TYPE_GATEWAY, "s3-gw"));

    @Test
    void clientTokenAuthenticatesAgainstServer() throws Exception {
        SaslClientAuthenticator client = provider.newClientAuthenticator("alice", "wonderland");
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] challenge = server.evaluateResponse(client.initialResponse());
        assertEquals(0, challenge.length);
        assertTrue(server.isComplete());
        assertTrue(client.isComplete());
        assertEquals(Principal.user("alice"), server.principal());
    }

    @Test
    void principalMappingIsApplied() throws Exception {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        server.evaluateResponse(provider.newClientAuthenticator("s3-gw", "gateway-pw")
                .initialResponse());
        assertEquals(new Principal(Principal.TYPE_GATEWAY, "s3-gw"), server.principal());
    }

    @Test
    void wrongPasswordIsRejected() {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] token = provider.newClientAuthenticator("alice", "nope").initialResponse();
        assertThrows(AuthenticationException.class, () -> server.evaluateResponse(token));
    }

    @Test
    void unknownUserIsRejectedWithTheSameMessageAsWrongPassword() {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] token = provider.newClientAuthenticator("mallory", "x").initialResponse();
        AuthenticationException unknown =
                assertThrows(AuthenticationException.class, () -> server.evaluateResponse(token));
        SaslServerAuthenticator server2 = provider.newServerAuthenticator(store);
        byte[] token2 = provider.newClientAuthenticator("alice", "x").initialResponse();
        AuthenticationException wrongPw =
                assertThrows(AuthenticationException.class, () -> server2.evaluateResponse(token2));
        assertEquals(wrongPw.getMessage(), unknown.getMessage());
    }

    @Test
    void malformedTokenIsRejected() {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        assertThrows(AuthenticationException.class,
                () -> server.evaluateResponse("no separators".getBytes(StandardCharsets.UTF_8)));
    }
}
