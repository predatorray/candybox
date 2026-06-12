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
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** The RFC 5802 cryptographic primitives for SCRAM-SHA-256 (RFC 7677), all JDK-built-in. */
final class ScramFunctions {

    private static final String HMAC_ALG = "HmacSHA256";
    private static final String HASH_ALG = "SHA-256";
    private static final byte[] CLIENT_KEY_INFO = "Client Key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERVER_KEY_INFO = "Server Key".getBytes(StandardCharsets.UTF_8);

    private ScramFunctions() {
    }

    /** {@code Hi(password, salt, i)} — PBKDF2-HMAC-SHA256. */
    static byte[] saltedPassword(String password, byte[] salt, int iterations) {
        return Passwords.pbkdf2(password, salt, iterations);
    }

    static byte[] hmac(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALG);
            mac.init(new SecretKeySpec(key, HMAC_ALG));
            return mac.doFinal(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(HMAC_ALG + " unavailable", e);
        }
    }

    static byte[] h(byte[] data) {
        try {
            return MessageDigest.getInstance(HASH_ALG).digest(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(HASH_ALG + " unavailable", e);
        }
    }

    static byte[] clientKey(byte[] saltedPassword) {
        return hmac(saltedPassword, CLIENT_KEY_INFO);
    }

    static byte[] serverKey(byte[] saltedPassword) {
        return hmac(saltedPassword, SERVER_KEY_INFO);
    }

    static byte[] xor(byte[] a, byte[] b) {
        byte[] out = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            out[i] = (byte) (a[i] ^ b[i]);
        }
        return out;
    }
}
