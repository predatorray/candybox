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
package me.predatorray.candybox.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SecurityConfigTest {

    private static SecurityConfig of(Map<String, String> keys) {
        return SecurityConfig.resolve(k -> Optional.ofNullable(keys.get(k)));
    }

    @Test
    void defaultsAreAllOff() {
        SecurityConfig c = SecurityConfig.disabled();
        assertFalse(c.authEnabled());
        assertFalse(c.tlsEnabled());
        assertNull(c.clientUsername());
        assertNull(c.serverSslContext());
        assertNull(c.clientSslContext());
        assertEquals(SecurityConfig.DEFAULT_MECHANISMS, c.saslMechanisms());
    }

    @Test
    void parsesTheFullSurface() {
        SecurityConfig c = of(Map.of(
                "auth.enabled", "true",
                "auth.required", "false",
                "auth.sasl.mechanisms", "SCRAM-SHA-256",
                "auth.credentials.file", "/etc/candybox/creds.properties",
                "auth.super.users", "Gateway:s3-gw, Admin:ops",
                "auth.client.username", "node-1",
                "auth.client.password", "pw",
                "tls.enabled", "true",
                "tls.cert.path", "/tls/tls.crt",
                "tls.key.path", "/tls/tls.key"));
        assertTrue(c.authEnabled());
        assertFalse(c.authRequired());
        assertEquals(List.of("SCRAM-SHA-256"), c.saslMechanisms());
        assertEquals(List.of("Gateway:s3-gw", "Admin:ops"), c.superUsers());
        assertEquals("node-1", c.clientUsername());
        assertEquals("pw", c.clientPassword());
        assertTrue(c.tlsEnabled());
        assertTrue(c.tlsVerifyEndpoint());
    }

    @Test
    void authNeedsACredentialsFile() {
        assertThrows(IllegalArgumentException.class, () -> of(Map.of("auth.enabled", "true")));
    }

    @Test
    void clientUsernameNeedsAPassword() {
        assertThrows(IllegalArgumentException.class,
                () -> of(Map.of("auth.client.username", "node-1")));
    }

    @Test
    void tlsListenerNeedsCertAndKey() {
        SecurityConfig c = of(Map.of("tls.enabled", "true"));
        assertThrows(IllegalArgumentException.class, c::serverSslContext);
    }
}
