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

/**
 * The server half of one SASL exchange on one connection. Tokens are opaque on the wire; their
 * format is defined by the mechanism's RFC, so third-party SASL clients interoperate.
 */
public interface SaslServerAuthenticator {

    /**
     * Consumes the client's next token and produces the server's challenge (possibly empty). After
     * the call that makes {@link #isComplete()} true, the returned bytes — e.g. SCRAM's
     * server-final message — must still be delivered to the client.
     *
     * @param response the client token
     * @return the challenge to send back
     * @throws AuthenticationException if the token is malformed or the credentials are wrong
     */
    byte[] evaluateResponse(byte[] response) throws AuthenticationException;

    /** True once the exchange succeeded and {@link #principal()} is available. */
    boolean isComplete();

    /** The authenticated principal; only valid once {@link #isComplete()}. */
    Principal principal();
}
