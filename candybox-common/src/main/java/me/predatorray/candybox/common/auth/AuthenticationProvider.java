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

/**
 * A pluggable SASL mechanism: a factory for the per-connection server and client authenticators.
 * Built-ins are {@code PLAIN} and {@code SCRAM-SHA-256}; additional mechanisms (e.g. OAUTHBEARER)
 * can be registered via {@link java.util.ServiceLoader} — see {@link AuthenticationProviders}.
 */
public interface AuthenticationProvider {

    /** The SASL mechanism name as negotiated on the wire (e.g. {@code "SCRAM-SHA-256"}). */
    String mechanism();

    /** A fresh server-side authenticator verifying against the given store. */
    SaslServerAuthenticator newServerAuthenticator(CredentialStore credentials);

    /** A fresh client-side authenticator for the given login. */
    SaslClientAuthenticator newClientAuthenticator(String username, String password);
}
