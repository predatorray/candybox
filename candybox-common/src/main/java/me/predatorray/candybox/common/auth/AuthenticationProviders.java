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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Resolves mechanism names to {@link AuthenticationProvider}s: the built-in {@code PLAIN} and
 * {@code SCRAM-SHA-256}, plus any provider registered through {@link ServiceLoader} (a
 * {@code META-INF/services/me.predatorray.candybox.common.auth.AuthenticationProvider} entry on the
 * classpath), which may also override a built-in mechanism.
 */
public final class AuthenticationProviders {

    private AuthenticationProviders() {
    }

    /** All known providers keyed by mechanism, ServiceLoader registrations winning over built-ins. */
    public static Map<String, AuthenticationProvider> discover() {
        Map<String, AuthenticationProvider> byMechanism = new LinkedHashMap<>();
        byMechanism.put(PlainAuthenticationProvider.MECHANISM, new PlainAuthenticationProvider());
        byMechanism.put(ScramSha256AuthenticationProvider.MECHANISM,
                new ScramSha256AuthenticationProvider());
        for (AuthenticationProvider custom : ServiceLoader.load(AuthenticationProvider.class)) {
            byMechanism.put(custom.mechanism(), custom);
        }
        return byMechanism;
    }

    /**
     * The providers for an enabled-mechanism list (insertion order preserved).
     *
     * @throws IllegalArgumentException if a mechanism has no provider
     */
    public static Map<String, AuthenticationProvider> forMechanisms(List<String> mechanisms) {
        Map<String, AuthenticationProvider> all = discover();
        Map<String, AuthenticationProvider> enabled = new LinkedHashMap<>();
        for (String mechanism : mechanisms) {
            String name = mechanism.trim();
            AuthenticationProvider provider = all.get(name);
            if (provider == null) {
                throw new IllegalArgumentException("No AuthenticationProvider for SASL mechanism '"
                        + name + "' (known: " + all.keySet() + ")");
            }
            enabled.put(name, provider);
        }
        return enabled;
    }
}
