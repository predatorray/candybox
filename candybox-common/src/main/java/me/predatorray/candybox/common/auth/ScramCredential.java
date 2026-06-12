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

import java.security.SecureRandom;
import java.util.Base64;

/**
 * A server-side SCRAM credential (RFC 5802): the salt + iteration count handed to the client and
 * the derived {@code StoredKey}/{@code ServerKey} — the password itself is <em>not</em>
 * recoverable from it. Stored in the credential file as
 * {@code salt=<b64>,iterations=<n>,storedKey=<b64>,serverKey=<b64>}.
 */
public record ScramCredential(byte[] salt, int iterations, byte[] storedKey, byte[] serverKey) {

    private static final int DEFAULT_ITERATIONS = 4096;
    private static final int SALT_BYTES = 16;

    /** Derives a credential from a password with a fresh random salt. */
    public static ScramCredential fromPassword(String password) {
        return fromPassword(password, DEFAULT_ITERATIONS);
    }

    static ScramCredential fromPassword(String password, int iterations) {
        byte[] salt = new byte[SALT_BYTES];
        new SecureRandom().nextBytes(salt);
        byte[] saltedPassword = ScramFunctions.saltedPassword(password, salt, iterations);
        byte[] clientKey = ScramFunctions.clientKey(saltedPassword);
        return new ScramCredential(salt, iterations,
                ScramFunctions.h(clientKey), ScramFunctions.serverKey(saltedPassword));
    }

    /** Renders the credential-file form. */
    public String toFileString() {
        Base64.Encoder b64 = Base64.getEncoder();
        return "salt=" + b64.encodeToString(salt) + ",iterations=" + iterations
                + ",storedKey=" + b64.encodeToString(storedKey)
                + ",serverKey=" + b64.encodeToString(serverKey);
    }

    /** Parses the credential-file form. */
    public static ScramCredential parse(String s) {
        byte[] salt = null;
        byte[] storedKey = null;
        byte[] serverKey = null;
        int iterations = -1;
        for (String part : s.split(",")) {
            int eq = part.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("Malformed SCRAM credential attribute: " + part);
            }
            String name = part.substring(0, eq).trim();
            String value = part.substring(eq + 1).trim();
            switch (name) {
                case "salt" -> salt = Base64.getDecoder().decode(value);
                case "iterations" -> iterations = Integer.parseInt(value);
                case "storedKey" -> storedKey = Base64.getDecoder().decode(value);
                case "serverKey" -> serverKey = Base64.getDecoder().decode(value);
                default -> throw new IllegalArgumentException(
                        "Unknown SCRAM credential attribute: " + name);
            }
        }
        if (salt == null || storedKey == null || serverKey == null || iterations <= 0) {
            throw new IllegalArgumentException(
                    "SCRAM credential requires salt, iterations, storedKey and serverKey");
        }
        return new ScramCredential(salt, iterations, storedKey, serverKey);
    }
}
