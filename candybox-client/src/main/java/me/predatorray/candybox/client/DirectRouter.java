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
package me.predatorray.candybox.client;

import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;

/**
 * A {@link Router} that talks to a single fixed node over one connection, ignoring Box ownership. Used
 * for single-node deployments and tests; a {@code MOVED} response is passed straight through (the
 * {@link CandyboxClient} surfaces it as a {@code NotOwnerException} rather than re-routing).
 */
final class DirectRouter implements Router {

    private final Connection connection;
    private final MessageCodec codec = new MessageCodec();

    DirectRouter(Transport transport, String host, int port) {
        this.connection = transport.connect(host, port);
    }

    @Override
    public Message callBox(String box, Message request) {
        return send(request);
    }

    @Override
    public Message callAny(Message request) {
        return send(request);
    }

    private Message send(Message request) {
        Frame response = connection.call(codec.encode(request));
        return codec.decode(response);
    }

    @Override
    public void close() {
        connection.close();
    }
}
