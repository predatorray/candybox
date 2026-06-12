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

import java.nio.charset.StandardCharsets;
import me.predatorray.candybox.common.exception.AuthenticationException;

/**
 * SASL {@code PLAIN} (RFC 4616): a single client message {@code [authzid] NUL authcid NUL passwd}.
 * The password crosses the wire, so PLAIN is intended for TLS-protected listeners — in exchange the
 * server side verifies against a one-way {@link Passwords} hash, never a stored cleartext password.
 * The optional authzid is rejected (no impersonation support).
 */
public final class PlainAuthenticationProvider implements AuthenticationProvider {

    public static final String MECHANISM = "PLAIN";

    private static final String NUL = "\0";

    @Override
    public String mechanism() {
        return MECHANISM;
    }

    @Override
    public SaslServerAuthenticator newServerAuthenticator(CredentialStore credentials) {
        return new Server(credentials);
    }

    @Override
    public SaslClientAuthenticator newClientAuthenticator(String username, String password) {
        return new Client(username, password);
    }

    private static final class Server implements SaslServerAuthenticator {
        private final CredentialStore credentials;
        private Principal principal;

        Server(CredentialStore credentials) {
            this.credentials = credentials;
        }

        @Override
        public byte[] evaluateResponse(byte[] response) throws AuthenticationException {
            if (isComplete()) {
                throw new AuthenticationException("PLAIN exchange already complete");
            }
            String message = new String(response, StandardCharsets.UTF_8);
            String[] parts = message.split(NUL, -1);
            if (parts.length != 3) {
                throw new AuthenticationException("Malformed PLAIN token");
            }
            String authzid = parts[0];
            String username = parts[1];
            String password = parts[2];
            if (!authzid.isEmpty() && !authzid.equals(username)) {
                throw new AuthenticationException("PLAIN authzid is not supported");
            }
            // Verify even when the user is unknown (against an unsatisfiable verifier) so response
            // timing does not reveal which usernames exist.
            String verifier = credentials.plainVerifier(username).orElse(null);
            boolean ok = Passwords.verify(password, verifier == null ? "pbkdf2-sha256:!" : verifier)
                    && verifier != null;
            if (username.isEmpty() || !ok) {
                throw new AuthenticationException("Authentication failed");
            }
            this.principal = credentials.principalOf(username);
            return new byte[0];
        }

        @Override
        public boolean isComplete() {
            return principal != null;
        }

        @Override
        public Principal principal() {
            if (principal == null) {
                throw new IllegalStateException("PLAIN exchange not complete");
            }
            return principal;
        }
    }

    private static final class Client implements SaslClientAuthenticator {
        private final String username;
        private final String password;
        private boolean sent;

        Client(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public byte[] initialResponse() {
            sent = true;
            return (NUL + username + NUL + password).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] evaluateChallenge(byte[] challenge) throws AuthenticationException {
            throw new AuthenticationException("PLAIN expects no server challenge");
        }

        @Override
        public boolean isComplete() {
            return sent;
        }
    }
}
