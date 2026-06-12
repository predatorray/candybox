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

import java.util.Optional;

/**
 * Where server-side authenticators look up credentials. The file-backed implementation is
 * {@link FileCredentialStore}; tests use {@link InMemoryCredentialStore}.
 */
public interface CredentialStore {

    /** The stored {@link Passwords} verifier for a SASL PLAIN user, if the user exists. */
    Optional<String> plainVerifier(String username);

    /** The stored SCRAM-SHA-256 credential for a user, if one was provisioned. */
    Optional<ScramCredential> scramCredential(String username);

    /**
     * The principal an authenticated username maps to. Defaults to {@code User:<username>}; a store
     * may override (e.g. mapping the {@code s3-gw} login to {@code Gateway:s3-gw}).
     */
    default Principal principalOf(String username) {
        return Principal.user(username);
    }
}
