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

import java.util.List;
import org.junit.jupiter.api.Test;

class PrincipalAndProvidersTest {

    @Test
    void principalParsesTypedAndBareForms() {
        assertEquals(new Principal("Gateway", "s3-gw"), Principal.parse("Gateway:s3-gw"));
        assertEquals(Principal.user("alice"), Principal.parse("alice"));
        assertEquals("Node:3", new Principal(Principal.TYPE_NODE, "3").toString());
        assertTrue(Principal.ANONYMOUS.isAnonymous());
        assertFalse(Principal.user("alice").isAnonymous());
    }

    @Test
    void principalRejectsBlankAndColonedTypes() {
        assertThrows(IllegalArgumentException.class, () -> new Principal("", "x"));
        assertThrows(IllegalArgumentException.class, () -> new Principal("User", " "));
        assertThrows(IllegalArgumentException.class, () -> new Principal("Us:er", "x"));
    }

    @Test
    void providersResolveBuiltinsAndRejectUnknownMechanisms() {
        var providers = AuthenticationProviders.forMechanisms(List.of("PLAIN", "SCRAM-SHA-256"));
        assertEquals(2, providers.size());
        assertEquals("PLAIN", providers.get("PLAIN").mechanism());

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> AuthenticationProviders.forMechanisms(List.of("OAUTHBEARER")));
        assertTrue(e.getMessage().contains("OAUTHBEARER"));
        assertTrue(e.getMessage().contains("PLAIN")); // lists what IS known
    }

    @Test
    void objectAclMatchesOwnerAndGrantsButNeverWhenUnowned() {
        ObjectAcl owned = new ObjectAcl("User:alice", List.of(Grant.parse("AllUsers:READ")));
        assertTrue(owned.permits(Principal.user("alice"), Operation.WRITE_ACP));
        assertTrue(owned.permits(Principal.ANONYMOUS, Operation.READ));
        assertFalse(owned.permits(Principal.user("bob"), Operation.WRITE_ACP));
        assertFalse(ObjectAcl.NONE.permits(Principal.user("alice"), Operation.READ));
        assertEquals("User:alice", ObjectAcl.ownedBy(Principal.user("alice")).owner());
    }

    @Test
    void allowAllAuthorizerPermitsEverythingIncludingOwnerOverrides() {
        assertTrue(Authorizer.ALLOW_ALL.authorize(Principal.ANONYMOUS, Operation.ADMIN,
                Resource.box("any")));
        assertTrue(Authorizer.ALLOW_ALL.isSuperUser(Principal.ANONYMOUS));
        assertEquals("Cluster", Resource.CLUSTER.toString());
        assertEquals("Box:b", Resource.box("b").toString());
    }
}
