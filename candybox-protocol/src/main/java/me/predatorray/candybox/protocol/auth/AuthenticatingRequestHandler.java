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

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.auth.AuthenticationProvider;
import me.predatorray.candybox.common.auth.CredentialStore;
import me.predatorray.candybox.common.auth.SaslServerAuthenticator;
import me.predatorray.candybox.common.exception.AuthenticationException;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.Opcode;
import me.predatorray.candybox.protocol.transport.ConnectionContext;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The server-side SASL gate (Kafka KIP-43/KIP-152 shape), wrapped around the node's real
 * dispatcher: it owns the {@code SASL_HANDSHAKE} / {@code SASL_AUTHENTICATE} opcodes, runs the
 * per-connection mechanism exchange, stamps the authenticated {@link ConnectionContext}, and —
 * when authentication is required — answers any other opcode from an unauthenticated connection
 * with {@code RESPONSE_AUTH_FAILED} instead of forwarding it.
 *
 * <p>Failure responses are deliberately uninformative ("Authentication failed"): the specific
 * reason (unknown user vs. wrong password vs. malformed token) is only logged server-side.
 */
public final class AuthenticatingRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticatingRequestHandler.class);

    private final RequestHandler delegate;
    private final Map<String, AuthenticationProvider> mechanisms;
    private final CredentialStore credentials;
    private final boolean required;
    private final MessageCodec codec = new MessageCodec();

    /**
     * @param delegate   the real dispatcher, invoked only for authenticated (or, when not required,
     *                   anonymous) connections
     * @param mechanisms the enabled mechanisms in preference order
     * @param credentials where to verify against
     * @param required   true ⇒ every non-SASL opcode needs a completed exchange first; false ⇒
     *                   unauthenticated connections pass through as anonymous (the exchange is
     *                   still offered)
     */
    public AuthenticatingRequestHandler(RequestHandler delegate,
                                        Map<String, AuthenticationProvider> mechanisms,
                                        CredentialStore credentials, boolean required) {
        this.delegate = delegate;
        this.mechanisms = Map.copyOf(mechanisms);
        this.credentials = credentials;
        this.required = required;
    }

    @Override
    public Frame handle(Frame request) {
        return handle(new ConnectionContext(), request);
    }

    @Override
    public Frame handle(ConnectionContext context, Frame request) {
        Opcode opcode = request.opcode();
        if (opcode == Opcode.SASL_HANDSHAKE) {
            return handshake(context, request);
        }
        if (opcode == Opcode.SASL_AUTHENTICATE) {
            return authenticate(context, request);
        }
        if (required && !context.isAuthenticated()) {
            return authFailed("Not authenticated: this server requires SASL authentication "
                    + "(mechanisms: " + List.copyOf(mechanisms.keySet()) + ")");
        }
        return delegate.handle(context, request);
    }

    private Frame handshake(ConnectionContext context, Frame request) {
        if (context.isAuthenticated()) {
            return authFailed("Connection is already authenticated");
        }
        Message decoded;
        try {
            decoded = codec.decode(request);
        } catch (RuntimeException e) {
            return authFailed("Malformed SASL handshake");
        }
        String mechanism = ((Message.SaslHandshakeRequest) decoded).mechanism();
        List<String> enabled = List.copyOf(mechanisms.keySet());
        AuthenticationProvider provider = mechanisms.get(mechanism);
        if (provider == null) {
            LOG.debug("Rejected SASL handshake for disabled mechanism {}", mechanism);
            return codec.encode(new Message.SaslHandshakeResponse(false, enabled));
        }
        context.saslState(provider.newServerAuthenticator(credentials));
        return codec.encode(new Message.SaslHandshakeResponse(true, enabled));
    }

    private Frame authenticate(ConnectionContext context, Frame request) {
        if (context.isAuthenticated()) {
            return authFailed("Connection is already authenticated");
        }
        if (!(context.saslState() instanceof SaslServerAuthenticator authenticator)) {
            return authFailed("SASL_AUTHENTICATE before SASL_HANDSHAKE");
        }
        Message decoded;
        try {
            decoded = codec.decode(request);
        } catch (RuntimeException e) {
            return authFailed("Malformed SASL token");
        }
        byte[] token = ((Message.SaslAuthenticateRequest) decoded).token();
        try {
            byte[] challenge = authenticator.evaluateResponse(token);
            boolean complete = authenticator.isComplete();
            if (complete) {
                context.authenticated(authenticator.principal());
                LOG.debug("Connection authenticated as {}", context.principal());
            }
            return codec.encode(new Message.SaslAuthenticateResponse(complete, challenge));
        } catch (AuthenticationException e) {
            LOG.info("SASL authentication failed: {}", e.getMessage());
            context.saslState(null); // the exchange is dead; a retry needs a fresh handshake
            return authFailed("Authentication failed");
        }
    }

    private Frame authFailed(String message) {
        return codec.encode(new Message.AuthFailedResponse(message));
    }
}
