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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import me.predatorray.candybox.common.exception.AuthenticationException;

/**
 * SASL {@code SCRAM-SHA-256} (RFC 5802 / RFC 7677), without channel binding ({@code n,,}). The
 * password never crosses the wire — both sides prove knowledge of it via HMAC challenges — and the
 * client verifies the server's signature too (mutual authentication), so SCRAM remains safe on a
 * listener whose TLS is terminated elsewhere. Message formats follow the RFC exactly, so
 * third-party SCRAM clients interoperate.
 *
 * <p>Usernames are restricted to ASCII without {@code ,} or {@code =} instead of implementing
 * SASLprep; the {@code =2C}/{@code =3D} escapes are still decoded for client compatibility.
 */
public final class ScramSha256AuthenticationProvider implements AuthenticationProvider {

    public static final String MECHANISM = "SCRAM-SHA-256";

    /** base64("n,,") — the channel-binding attribute the client-final message must carry. */
    private static final String GS2_NO_CHANNEL_BINDING_B64 = "biws";
    private static final int NONCE_BYTES = 24;

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

    private static String newNonce() {
        byte[] bytes = new byte[NONCE_BYTES];
        new SecureRandom().nextBytes(bytes);
        // Printable, '=',','-free by construction.
        return new BigInteger(1, bytes).toString(36);
    }

    /** Parses {@code a=v,b=v,...}; values may contain '=' (base64), so split on the first only. */
    private static Map<String, String> parseAttributes(String message)
            throws AuthenticationException {
        Map<String, String> attrs = new LinkedHashMap<>();
        for (String part : message.split(",", -1)) {
            if (part.isEmpty()) {
                continue;
            }
            if (part.length() < 2 || part.charAt(1) != '=') {
                throw new AuthenticationException("Malformed SCRAM attribute: " + part);
            }
            attrs.put(part.substring(0, 1), part.substring(2));
        }
        return attrs;
    }

    private static String saslName(String username) {
        return username.replace("=", "=3D").replace(",", "=2C");
    }

    private static String decodeSaslName(String name) {
        return name.replace("=2C", ",").replace("=3D", "=");
    }

    private static final class Server implements SaslServerAuthenticator {
        private final CredentialStore credentials;

        private String clientFirstBare;
        private String serverFirst;
        private String username;
        private String combinedNonce;
        private ScramCredential credential;
        private Principal principal;

        Server(CredentialStore credentials) {
            this.credentials = credentials;
        }

        @Override
        public byte[] evaluateResponse(byte[] response) throws AuthenticationException {
            if (principal != null) {
                throw new AuthenticationException("SCRAM exchange already complete");
            }
            String message = new String(response, StandardCharsets.UTF_8);
            if (clientFirstBare == null) {
                return clientFirst(message).getBytes(StandardCharsets.UTF_8);
            }
            return clientFinal(message).getBytes(StandardCharsets.UTF_8);
        }

        private String clientFirst(String message) throws AuthenticationException {
            // client-first-message = gs2-header client-first-message-bare ; gs2-header = "n,,"
            if (!message.startsWith("n,,")) {
                throw new AuthenticationException(
                        "SCRAM channel binding is not supported (expected gs2-header 'n,,')");
            }
            clientFirstBare = message.substring(3);
            Map<String, String> attrs = parseAttributes(clientFirstBare);
            String name = attrs.get("n");
            String clientNonce = attrs.get("r");
            if (name == null || clientNonce == null) {
                throw new AuthenticationException("Malformed SCRAM client-first message");
            }
            username = decodeSaslName(name);
            // For an unknown user proceed with a fake credential and fail at the proof check, so
            // the exchange shape does not reveal which usernames exist. The fake salt is derived
            // from the username so repeated probes see a stable salt, like a real account.
            var stored = credentials.scramCredential(username);
            credential = stored.orElseGet(() -> fakeCredential(username));
            if (stored.isEmpty()) {
                username = null;
            }
            combinedNonce = clientNonce + newNonce();
            serverFirst = "r=" + combinedNonce
                    + ",s=" + Base64.getEncoder().encodeToString(credential.salt())
                    + ",i=" + credential.iterations();
            return serverFirst;
        }

        /** An unsatisfiable credential (no ClientKey hashes to an all-zero StoredKey). */
        private static ScramCredential fakeCredential(String username) {
            byte[] salt = ScramFunctions.h(
                    ("candybox-scram-unknown-user:" + username).getBytes(StandardCharsets.UTF_8));
            return new ScramCredential(salt, 4096, new byte[32], new byte[32]);
        }

        private String clientFinal(String message) throws AuthenticationException {
            Map<String, String> attrs = parseAttributes(message);
            String channelBinding = attrs.get("c");
            String nonce = attrs.get("r");
            String proofB64 = attrs.get("p");
            if (proofB64 == null || nonce == null
                    || !GS2_NO_CHANNEL_BINDING_B64.equals(channelBinding)) {
                throw new AuthenticationException("Malformed SCRAM client-final message");
            }
            if (!combinedNonce.equals(nonce)) {
                throw new AuthenticationException("SCRAM nonce mismatch");
            }
            String clientFinalWithoutProof =
                    message.substring(0, message.lastIndexOf(",p="));
            String authMessage =
                    clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof;
            byte[] authMessageBytes = authMessage.getBytes(StandardCharsets.UTF_8);

            byte[] clientProof;
            try {
                clientProof = Base64.getDecoder().decode(proofB64);
            } catch (IllegalArgumentException e) {
                throw new AuthenticationException("Malformed SCRAM client proof");
            }
            byte[] clientSignature = ScramFunctions.hmac(credential.storedKey(), authMessageBytes);
            if (clientProof.length != clientSignature.length) {
                throw new AuthenticationException("Authentication failed");
            }
            byte[] recoveredClientKey = ScramFunctions.xor(clientProof, clientSignature);
            boolean ok = MessageDigest.isEqual(
                    ScramFunctions.h(recoveredClientKey), credential.storedKey());
            if (!ok || username == null) {
                throw new AuthenticationException("Authentication failed");
            }
            principal = credentials.principalOf(username);
            byte[] serverSignature = ScramFunctions.hmac(credential.serverKey(), authMessageBytes);
            return "v=" + Base64.getEncoder().encodeToString(serverSignature);
        }

        @Override
        public boolean isComplete() {
            return principal != null;
        }

        @Override
        public Principal principal() {
            if (principal == null) {
                throw new IllegalStateException("SCRAM exchange not complete");
            }
            return principal;
        }
    }

    private static final class Client implements SaslClientAuthenticator {
        private final String username;
        private final String password;
        private final String clientNonce = newNonce();

        private String clientFirstBare;
        private byte[] saltedPassword;
        private String expectedServerSignatureB64;
        private boolean complete;

        Client(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public byte[] initialResponse() {
            clientFirstBare = "n=" + saslName(username) + ",r=" + clientNonce;
            return ("n,," + clientFirstBare).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] evaluateChallenge(byte[] challenge) throws AuthenticationException {
            String message = new String(challenge, StandardCharsets.UTF_8);
            if (expectedServerSignatureB64 == null) {
                return serverFirst(message).getBytes(StandardCharsets.UTF_8);
            }
            serverFinal(message);
            return new byte[0];
        }

        private String serverFirst(String serverFirst) throws AuthenticationException {
            Map<String, String> attrs = parseAttributes(serverFirst);
            String nonce = attrs.get("r");
            String saltB64 = attrs.get("s");
            String iterations = attrs.get("i");
            if (nonce == null || saltB64 == null || iterations == null) {
                throw new AuthenticationException("Malformed SCRAM server-first message");
            }
            if (!nonce.startsWith(clientNonce)) {
                throw new AuthenticationException("SCRAM server nonce does not extend ours");
            }
            byte[] salt;
            int i;
            try {
                salt = Base64.getDecoder().decode(saltB64);
                i = Integer.parseInt(iterations);
            } catch (RuntimeException e) {
                throw new AuthenticationException("Malformed SCRAM server-first message");
            }
            saltedPassword = ScramFunctions.saltedPassword(password, salt, i);

            String clientFinalWithoutProof = "c=" + GS2_NO_CHANNEL_BINDING_B64 + ",r=" + nonce;
            String authMessage = clientFirstBare + "," + serverFirst + ","
                    + clientFinalWithoutProof;
            byte[] authMessageBytes = authMessage.getBytes(StandardCharsets.UTF_8);

            byte[] clientKey = ScramFunctions.clientKey(saltedPassword);
            byte[] storedKey = ScramFunctions.h(clientKey);
            byte[] clientSignature = ScramFunctions.hmac(storedKey, authMessageBytes);
            byte[] proof = ScramFunctions.xor(clientKey, clientSignature);

            byte[] serverKey = ScramFunctions.serverKey(saltedPassword);
            expectedServerSignatureB64 = Base64.getEncoder()
                    .encodeToString(ScramFunctions.hmac(serverKey, authMessageBytes));

            return clientFinalWithoutProof + ",p=" + Base64.getEncoder().encodeToString(proof);
        }

        private void serverFinal(String serverFinal) throws AuthenticationException {
            Map<String, String> attrs = parseAttributes(serverFinal);
            String verifier = attrs.get("v");
            if (verifier == null) {
                throw new AuthenticationException(
                        "SCRAM server-final error: " + attrs.getOrDefault("e", serverFinal));
            }
            if (!MessageDigest.isEqual(verifier.getBytes(StandardCharsets.UTF_8),
                    expectedServerSignatureB64.getBytes(StandardCharsets.UTF_8))) {
                throw new AuthenticationException(
                        "SCRAM server signature mismatch (server does not know the password)");
            }
            complete = true;
        }

        @Override
        public boolean isComplete() {
            return complete;
        }
    }
}
