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

import me.predatorray.candybox.protocol.Frame;

/** Server-side request handler: maps a request frame to a response frame. */
@FunctionalInterface
public interface RequestHandler {

    /**
     * Handles one request.
     *
     * @param request the decoded request frame
     * @return the response frame
     */
    Frame handle(Frame request);

    /**
     * Handles one request with its connection's state. Transports always invoke this form, with one
     * {@link ConnectionContext} per accepted connection; handlers that care about the caller's
     * identity (the SASL gate, the authorizing dispatcher) override it, while plain handlers and
     * test lambdas only implement {@link #handle(Frame)}.
     *
     * @param context the per-connection state (authentication progress + principal)
     * @param request the decoded request frame
     * @return the response frame
     */
    default Frame handle(ConnectionContext context, Frame request) {
        return handle(request);
    }
}
