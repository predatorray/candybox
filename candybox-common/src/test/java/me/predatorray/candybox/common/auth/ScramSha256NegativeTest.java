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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;
import me.predatorray.candybox.common.exception.AuthenticationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ScramSha256NegativeTest {
    private final ScramSha256AuthenticationProvider provider = new ScramSha256AuthenticationProvider();
    private final InMemoryCredentialStore store =
            new InMemoryCredentialStore().addUser("alice", "wonderland");

    @Test
    void attributeWithoutEqualsIsRejected() {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        String first = text(client().initialResponse()).replace("n=alice", "nfoo");
        assertThatThrownBy(() -> server.evaluateResponse(bytes(first)))
                .isInstanceOf(AuthenticationException.class).hasMessageContaining("Malformed SCRAM attribute");
        assertThat(server.isComplete()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"n", "r"})
    void missingClientFirstAttributeIsRejected(String attribute) {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        String first = "n,," + without(text(client().initialResponse()).substring(3), attribute);
        assertThatThrownBy(() -> server.evaluateResponse(bytes(first)))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("Malformed SCRAM client-first message");
        assertThat(server.isComplete()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"p", "r", "c"})
    void missingClientFinalAttributeIsRejected(String attribute) throws Exception {
        rejectClientFinal(attribute, null, "Malformed SCRAM client-final message");
    }

    @Test
    void unsupportedChannelBindingIsRejected() throws Exception {
        rejectClientFinal("c", "eA==", "Malformed SCRAM client-final message");
    }

    @Test
    void invalidBase64ProofIsRejected() throws Exception {
        rejectClientFinal("p", "!not-base64!", "Malformed SCRAM client proof");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "AA==", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="})
    void wrongProofLengthIsRejectedAsAuthenticationFailure(String proof) throws Exception {
        rejectClientFinal("p", proof, "Authentication failed");
    }

    private void rejectClientFinal(String attribute, String value, String error) throws Exception {
        SaslClientAuthenticator client = client();
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        String last = text(client.evaluateChallenge(server.evaluateResponse(client.initialResponse())));
        String hostile = value == null ? without(last, attribute) : replace(last, attribute, value);
        assertThatThrownBy(() -> server.evaluateResponse(bytes(hostile)))
                .isInstanceOf(AuthenticationException.class).hasMessageContaining(error);
        assertThat(server.isComplete()).isFalse();
    }

    @Test
    void completedServerRejectsAnotherResponse() throws Exception {
        SaslClientAuthenticator client = client();
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] last = client.evaluateChallenge(server.evaluateResponse(client.initialResponse()));
        client.evaluateChallenge(server.evaluateResponse(last));
        assertThat(client.isComplete()).isTrue();
        assertThat(server.isComplete()).isTrue();
        assertThatThrownBy(() -> server.evaluateResponse(last))
                .isInstanceOf(AuthenticationException.class).hasMessageContaining("SCRAM exchange already complete");
        assertThat(server.principal()).isEqualTo(Principal.user("alice"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"r", "s", "i"})
    void missingServerFirstAttributeIsRejected(String attribute) throws Exception {
        rejectServerFirst(attribute, null, "Malformed SCRAM server-first message");
    }

    @Test
    void serverNonceMustExtendClientNonce() throws Exception {
        rejectServerFirst("r", "evil", "SCRAM server nonce does not extend ours");
    }

    @Test
    void nonNumericIterationCountIsRejected() throws Exception {
        rejectServerFirst("i", "not-a-number", "Malformed SCRAM server-first message");
    }

    @Test
    void invalidBase64SaltIsRejected() throws Exception {
        rejectServerFirst("s", "!not-base64!", "Malformed SCRAM server-first message");
    }

    private void rejectServerFirst(String attribute, String value, String error) throws Exception {
        SaslClientAuthenticator client = client();
        String first = text(provider.newServerAuthenticator(store).evaluateResponse(client.initialResponse()));
        String hostile = value == null ? without(first, attribute) : replace(first, attribute, value);
        assertThatThrownBy(() -> client.evaluateChallenge(bytes(hostile)))
                .isInstanceOf(AuthenticationException.class).hasMessageContaining(error);
        assertThat(client.isComplete()).isFalse();
    }

    @Test
    void serverFinalErrorIsSurfacedWithoutCompletingClient() throws Exception {
        SaslClientAuthenticator client = client();
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] last = client.evaluateChallenge(server.evaluateResponse(client.initialResponse()));
        String serverFinal = text(server.evaluateResponse(last)).replaceFirst("v=.*", "e=invalid-proof");
        assertThatThrownBy(() -> client.evaluateChallenge(bytes(serverFinal)))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("SCRAM server-final error: invalid-proof");
        assertThat(client.isComplete()).isFalse();
    }

    private SaslClientAuthenticator client() {
        return provider.newClientAuthenticator("alice", "wonderland");
    }

    private static String without(String message, String attribute) {
        return Arrays.stream(message.split(","))
                .filter(part -> !part.startsWith(attribute + "="))
                .collect(Collectors.joining(","));
    }

    private static String replace(String message, String attribute, String value) {
        return message.replaceFirst(attribute + "=[^,]*", attribute + "=" + value);
    }

    private static byte[] bytes(String message) {
        return message.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] message) {
        return new String(message, StandardCharsets.UTF_8);
    }
}
