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

import me.predatorray.candybox.common.exception.AuthenticationException;

/** The client half of one SASL exchange on one connection. */
public interface SaslClientAuthenticator {

    /** The first token to send (mechanisms with a client-first message, i.e. PLAIN and SCRAM). */
    byte[] initialResponse() throws AuthenticationException;

    /**
     * Consumes a server challenge and produces the next token to send (possibly empty).
     *
     * @param challenge the server challenge
     * @return the next client token
     * @throws AuthenticationException if the challenge is malformed or fails verification (e.g. a
     *     SCRAM server signature mismatch — a server that doesn't know the password)
     */
    byte[] evaluateChallenge(byte[] challenge) throws AuthenticationException;

    /** True once the exchange succeeded, including any mutual-authentication verification. */
    boolean isComplete();
}
