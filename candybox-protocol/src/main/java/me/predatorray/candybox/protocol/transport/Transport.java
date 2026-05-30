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

/**
 * Client-side transport SPI: opens {@link Connection}s to nodes. Implemented by the real
 * {@link TcpTransport} and the in-JVM {@link LoopbackTransport} used in tests.
 */
public interface Transport extends AutoCloseable {

    /**
     * Opens a connection to a node.
     *
     * @param host node host
     * @param port node port
     * @return a connection
     */
    Connection connect(String host, int port);

    @Override
    void close();
}
