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
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Password hashing for the credential file: a stored verifier is
 * {@code pbkdf2-sha256:<iterations>:<saltB64>:<hashB64>} (the only form the {@code hash} side
 * produces), or {@code plain:<password>} accepted for dev/test fixtures. PBKDF2-HMAC-SHA256 is
 * JDK-built-in, so no third-party hashing dependency is introduced. Comparison is constant-time.
 */
public final class Passwords {

    private static final String PBKDF2_PREFIX = "pbkdf2-sha256:";
    private static final String PLAIN_PREFIX = "plain:";
    private static final int DEFAULT_ITERATIONS = 120_000;
    private static final int SALT_BYTES = 16;
    private static final int HASH_BITS = 256;

    private Passwords() {
    }

    /** Hashes a password into a stored verifier with a fresh random salt. */
    public static String hash(String password) {
        return hash(password, DEFAULT_ITERATIONS);
    }

    static String hash(String password, int iterations) {
        byte[] salt = new byte[SALT_BYTES];
        new SecureRandom().nextBytes(salt);
        byte[] dk = pbkdf2(password, salt, iterations);
        Base64.Encoder b64 = Base64.getEncoder();
        return PBKDF2_PREFIX + iterations + ":" + b64.encodeToString(salt) + ":"
                + b64.encodeToString(dk);
    }

    /** Verifies a password against a stored verifier; malformed verifiers verify as false. */
    public static boolean verify(String password, String storedVerifier) {
        if (password == null || storedVerifier == null) {
            return false;
        }
        if (storedVerifier.startsWith(PLAIN_PREFIX)) {
            return MessageDigest.isEqual(
                    password.getBytes(StandardCharsets.UTF_8),
                    storedVerifier.substring(PLAIN_PREFIX.length()).getBytes(StandardCharsets.UTF_8));
        }
        if (!storedVerifier.startsWith(PBKDF2_PREFIX)) {
            return false;
        }
        try {
            String[] parts = storedVerifier.substring(PBKDF2_PREFIX.length()).split(":");
            if (parts.length != 3) {
                return false;
            }
            int iterations = Integer.parseInt(parts[0]);
            byte[] salt = Base64.getDecoder().decode(parts[1]);
            byte[] expected = Base64.getDecoder().decode(parts[2]);
            return MessageDigest.isEqual(expected, pbkdf2(password, salt, iterations));
        } catch (RuntimeException malformed) {
            return false;
        }
    }

    static byte[] pbkdf2(String password, byte[] salt, int iterations) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, HASH_BITS);
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                    .generateSecret(spec).getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("PBKDF2WithHmacSHA256 unavailable", e);
        }
    }
}
