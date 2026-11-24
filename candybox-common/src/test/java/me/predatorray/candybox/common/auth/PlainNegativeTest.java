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
import me.predatorray.candybox.common.exception.AuthenticationException;
import org.junit.jupiter.api.Test;

class PlainNegativeTest {
    private final PlainAuthenticationProvider provider = new PlainAuthenticationProvider();
    private final InMemoryCredentialStore store =
            new InMemoryCredentialStore().addUser("alice", "wonderland");

    @Test
    void authzidCannotImpersonateAnotherUser() {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] valid = provider.newClientAuthenticator("alice", "wonderland").initialResponse();
        byte[] hostile = ("bob" + new String(valid, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> server.evaluateResponse(hostile))
                .isInstanceOf(AuthenticationException.class).hasMessageContaining("PLAIN authzid is not supported");
        assertThat(server.isComplete()).isFalse();
    }

    @Test
    void emptyUsernameIsRejected() {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] token = provider.newClientAuthenticator("", "wonderland").initialResponse();
        assertThatThrownBy(() -> server.evaluateResponse(token))
                .isInstanceOf(AuthenticationException.class).hasMessageContaining("Authentication failed");
        assertThat(server.isComplete()).isFalse();
    }

    @Test
    void completedServerRejectsAnotherResponse() throws Exception {
        SaslServerAuthenticator server = provider.newServerAuthenticator(store);
        byte[] token = provider.newClientAuthenticator("alice", "wonderland").initialResponse();
        server.evaluateResponse(token);
        assertThat(server.isComplete()).isTrue();
        assertThatThrownBy(() -> server.evaluateResponse(token))
                .isInstanceOf(AuthenticationException.class).hasMessageContaining("PLAIN exchange already complete");
        assertThat(server.principal()).isEqualTo(Principal.user("alice"));
    }

    @Test
    void clientRejectsServerChallenge() {
        SaslClientAuthenticator client = provider.newClientAuthenticator("alice", "wonderland");
        client.initialResponse();
        assertThatThrownBy(() -> client.evaluateChallenge(new byte[] {1}))
                .isInstanceOf(AuthenticationException.class).hasMessageContaining("PLAIN expects no server challenge");
    }
}
