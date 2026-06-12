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
package me.predatorray.candybox.protocol.auth;

import me.predatorray.candybox.common.auth.AuthenticationProvider;
import me.predatorray.candybox.common.auth.AuthenticationProviders;
import me.predatorray.candybox.common.auth.SaslClientAuthenticator;
import me.predatorray.candybox.common.exception.AuthenticationException;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;

/**
 * A client-side {@link Transport} decorator that runs the SASL exchange on every new connection
 * before handing it out, so routers/clients above stay authentication-unaware. Works over any
 * underlying transport (TCP with or without TLS, loopback in tests).
 */
public final class AuthenticatingTransport implements Transport {

    private final Transport delegate;
    private final AuthenticationProvider provider;
    private final String username;
    private final String password;
    private final MessageCodec codec = new MessageCodec();

    /**
     * @param mechanism the SASL mechanism to request (e.g. {@code "PLAIN"}, {@code "SCRAM-SHA-256"})
     */
    public AuthenticatingTransport(Transport delegate, String mechanism, String username,
                                   String password) {
        this.delegate = delegate;
        this.provider = AuthenticationProviders.forMechanisms(java.util.List.of(mechanism))
                .get(mechanism);
        this.username = username;
        this.password = password;
    }

    @Override
    public Connection connect(String host, int port) {
        Connection connection = delegate.connect(host, port);
        try {
            authenticate(connection);
            return connection;
        } catch (RuntimeException e) {
            connection.close();
            throw e;
        }
    }

    private void authenticate(Connection connection) {
        Message handshake = call(connection, new Message.SaslHandshakeRequest(provider.mechanism()));
        if (handshake instanceof Message.AuthFailedResponse failed) {
            throw new AuthenticationException(failed.message());
        }
        if (!(handshake instanceof Message.SaslHandshakeResponse response) || !response.ok()) {
            String enabled = handshake instanceof Message.SaslHandshakeResponse r
                    ? String.valueOf(r.enabledMechanisms()) : "unknown";
            throw new AuthenticationException("Server rejected SASL mechanism "
                    + provider.mechanism() + " (server enables: " + enabled + ")");
        }

        SaslClientAuthenticator authenticator =
                provider.newClientAuthenticator(username, password);
        byte[] token = authenticator.initialResponse();
        while (true) {
            Message reply = call(connection, new Message.SaslAuthenticateRequest(token));
            if (reply instanceof Message.AuthFailedResponse failed) {
                throw new AuthenticationException(failed.message());
            }
            if (!(reply instanceof Message.SaslAuthenticateResponse auth)) {
                throw new AuthenticationException("Unexpected SASL response: " + reply.opcode());
            }
            if (auth.complete()) {
                // Evaluate the final challenge too (SCRAM's server signature — mutual auth).
                if (auth.challenge().length > 0 && !authenticator.isComplete()) {
                    authenticator.evaluateChallenge(auth.challenge());
                }
                if (!authenticator.isComplete()) {
                    throw new AuthenticationException(
                            "Server completed the SASL exchange before the client did");
                }
                return;
            }
            token = authenticator.evaluateChallenge(auth.challenge());
        }
    }

    private Message call(Connection connection, Message request) {
        return codec.decode(connection.call(codec.encode(request)));
    }

    @Override
    public void close() {
        delegate.close();
    }
}
