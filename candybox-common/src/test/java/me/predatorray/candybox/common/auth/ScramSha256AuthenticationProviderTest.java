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

import java.nio.charset.StandardCharsets;
import me.predatorray.candybox.common.exception.AuthenticationException;
import org.junit.jupiter.api.Test;

class ScramSha256AuthenticationProviderTest {

    private final ScramSha256AuthenticationProvider provider =
            new ScramSha256AuthenticationProvider();
    private final InMemoryCredentialStore store =
            new InMemoryCredentialStore().addUser("alice", "wonderland");

    /** Runs the full 4-message exchange; returns the server authenticator for assertions. */
    private SaslServerAuthenticator exchange(String username, String password) throws Exception {
        SaslClientAuthenticator client = provider.newClientAuthenticator(username, password);
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] serverFirst = server.evaluateResponse(client.initialResponse());
        byte[] clientFinal = client.evaluateChallenge(serverFirst);
        byte[] serverFinal = server.evaluateResponse(clientFinal);
        client.evaluateChallenge(serverFinal);
        assertTrue(client.isComplete());
        return server;
    }

    @Test
    void fullExchangeAuthenticates() throws Exception {
        SaslServerAuthenticator server = exchange("alice", "wonderland");
        assertTrue(server.isComplete());
        assertEquals(Principal.user("alice"), server.principal());
    }

    @Test
    void wrongPasswordFailsAtTheProof() throws Exception {
        SaslClientAuthenticator client = provider.newClientAuthenticator("alice", "wrong");
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] serverFirst = server.evaluateResponse(client.initialResponse());
        byte[] clientFinal = client.evaluateChallenge(serverFirst);
        assertThrows(AuthenticationException.class, () -> server.evaluateResponse(clientFinal));
        assertFalse(server.isComplete());
    }

    @Test
    void unknownUserFailsAtTheProofNotEarlier() throws Exception {
        SaslClientAuthenticator client = provider.newClientAuthenticator("mallory", "whatever");
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        // The server must answer the first message normally (no user-enumeration oracle) ...
        byte[] serverFirst = server.evaluateResponse(client.initialResponse());
        byte[] clientFinal = client.evaluateChallenge(serverFirst);
        // ... and only reject the proof.
        assertThrows(AuthenticationException.class, () -> server.evaluateResponse(clientFinal));
    }

    @Test
    void unknownUserSeesAStableSaltAcrossProbes() throws Exception {
        String saltAttr = null;
        for (int i = 0; i < 2; i++) {
            SaslClientAuthenticator client = provider.newClientAuthenticator("mallory", "pw");
            SaslServerAuthenticator server = provider.newServerAuthenticator(store);
            String serverFirst = new String(server.evaluateResponse(client.initialResponse()),
                    StandardCharsets.UTF_8);
            String salt = serverFirst.split(",")[1];
            if (saltAttr == null) {
                saltAttr = salt;
            } else {
                assertEquals(saltAttr, salt);
            }
        }
    }

    @Test
    void clientDetectsAServerThatDoesNotKnowThePassword() throws Exception {
        SaslClientAuthenticator client = provider.newClientAuthenticator("alice", "wonderland");
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] serverFirst = server.evaluateResponse(client.initialResponse());
        client.evaluateChallenge(serverFirst);
        // A forged/buggy server-final signature must not complete the client (mutual auth).
        assertThrows(AuthenticationException.class, () -> client.evaluateChallenge(
                "v=Zm9yZ2VkLXNpZ25hdHVyZQ==".getBytes(StandardCharsets.UTF_8)));
        assertFalse(client.isComplete());
    }

    @Test
    void tamperedNonceIsRejected() throws Exception {
        SaslClientAuthenticator client = provider.newClientAuthenticator("alice", "wonderland");
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        String serverFirst = new String(server.evaluateResponse(client.initialResponse()),
                StandardCharsets.UTF_8);
        String clientFinal = new String(client.evaluateChallenge(
                serverFirst.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        String tampered = clientFinal.replaceFirst("r=[^,]+", "r=evil-nonce");
        assertThrows(AuthenticationException.class,
                () -> server.evaluateResponse(tampered.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void channelBindingGs2HeaderIsRequired() {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        assertThrows(AuthenticationException.class, () -> server.evaluateResponse(
                "p=tls-unique,,n=alice,r=abc".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void usernamesWithCommaAndEqualsRoundTripViaEscaping() throws Exception {
        store.addUser("we,ird=name", "pw");
        SaslServerAuthenticator server = exchange("we,ird=name", "pw");
        assertEquals(Principal.user("we,ird=name"), server.principal());
    }

    @Test
    void scramCredentialFileFormRoundTrips() {
        ScramCredential credential = ScramCredential.fromPassword("pw");
        ScramCredential parsed = ScramCredential.parse(credential.toFileString());
        assertEquals(credential.iterations(), parsed.iterations());
        assertTrue(java.util.Arrays.equals(credential.salt(), parsed.salt()));
        assertTrue(java.util.Arrays.equals(credential.storedKey(), parsed.storedKey()));
        assertTrue(java.util.Arrays.equals(credential.serverKey(), parsed.serverKey()));
    }
}
