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
package me.predatorray.candybox.protocol.transport;

import me.predatorray.candybox.common.auth.Principal;

/**
 * Server-side per-connection state, created by the transport when a connection is accepted and
 * passed to every {@link RequestHandler#handle(ConnectionContext, me.predatorray.candybox.protocol.Frame)}
 * call on that connection. Holds the authenticated {@link Principal} (null until the SASL exchange
 * completes) and the in-progress SASL authenticator owned by the authenticating handler.
 *
 * <p>Each connection is served by a single thread, but the fields are volatile so a handler chain
 * crossing threads still observes the authenticated state.
 */
public final class ConnectionContext {

    private volatile Principal principal;
    private volatile Object saslState;

    /** The authenticated principal, or null if the connection has not (yet) authenticated. */
    public Principal principal() {
        return principal;
    }

    /** The effective principal: the authenticated one, else {@link Principal#ANONYMOUS}. */
    public Principal principalOrAnonymous() {
        Principal p = principal;
        return p == null ? Principal.ANONYMOUS : p;
    }

    public boolean isAuthenticated() {
        return principal != null;
    }

    /** Marks the connection authenticated; only the authenticating handler calls this. */
    public void authenticated(Principal principal) {
        this.principal = principal;
        this.saslState = null;
    }

    /** The in-progress SASL exchange state (owned by the authenticating handler). */
    public Object saslState() {
        return saslState;
    }

    public void saslState(Object state) {
        this.saslState = state;
    }
}
