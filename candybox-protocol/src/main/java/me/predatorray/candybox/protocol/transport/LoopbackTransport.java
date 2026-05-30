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
import me.predatorray.candybox.protocol.FrameCodec;

/**
 * An in-JVM {@link Transport} that delivers requests straight to a {@link RequestHandler} with no
 * sockets. Requests and responses are still round-tripped through {@link FrameCodec} so tests
 * exercise the real wire encoding (including the max-frame cap) without networking.
 */
public final class LoopbackTransport implements Transport {

    private final RequestHandler handler;
    private final FrameCodec codec;

    public LoopbackTransport(RequestHandler handler) {
        this(handler, new FrameCodec());
    }

    public LoopbackTransport(RequestHandler handler, FrameCodec codec) {
        this.handler = handler;
        this.codec = codec;
    }

    @Override
    public Connection connect(String host, int port) {
        return new LoopbackConnection();
    }

    @Override
    public void close() {
        // nothing to release
    }

    private final class LoopbackConnection implements Connection {
        @Override
        public Frame call(Frame request) {
            Frame onWire = codec.decode(codec.encode(request)); // exercise the codec
            Frame response = handler.handle(onWire);
            return codec.decode(codec.encode(response));
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
