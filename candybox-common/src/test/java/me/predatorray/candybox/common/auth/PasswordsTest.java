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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PasswordsTest {

    @Test
    void hashedPasswordVerifies() {
        String verifier = Passwords.hash("s3cret");
        assertTrue(verifier.startsWith("pbkdf2-sha256:"));
        assertTrue(Passwords.verify("s3cret", verifier));
        assertFalse(Passwords.verify("wrong", verifier));
        assertFalse(Passwords.verify("", verifier));
    }

    @Test
    void saltsAreRandomSoHashesDiffer() {
        assertNotEquals(Passwords.hash("same"), Passwords.hash("same"));
    }

    @Test
    void plainVerifierWorksForDevFixtures() {
        assertTrue(Passwords.verify("dev-password", "plain:dev-password"));
        assertFalse(Passwords.verify("dev-passwore", "plain:dev-password"));
    }

    @Test
    void malformedVerifiersNeverVerify() {
        assertFalse(Passwords.verify("x", null));
        assertFalse(Passwords.verify("x", ""));
        assertFalse(Passwords.verify("x", "bcrypt:whatever"));
        assertFalse(Passwords.verify("x", "pbkdf2-sha256:!"));
        assertFalse(Passwords.verify("x", "pbkdf2-sha256:abc:def"));
        assertFalse(Passwords.verify("x", "pbkdf2-sha256:10:%%%:%%%"));
        assertFalse(Passwords.verify(null, "plain:x"));
    }
}
