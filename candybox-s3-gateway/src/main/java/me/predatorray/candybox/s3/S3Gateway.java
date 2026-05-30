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
package me.predatorray.candybox.s3;

import me.predatorray.candybox.client.CandyboxClient;

/**
 * Public facade for embedding a running S3 gateway over an existing cluster-aware {@link CandyboxClient}
 * — used by the process entrypoint ({@link S3GatewayMain}) and by integration tests that drive the
 * gateway against a real node. Does <em>not</em> own the supplied client's lifecycle (the caller closes
 * it); {@link #close()} stops only the Netty server.
 */
public final class S3Gateway implements AutoCloseable {

    private final S3GatewayServer server;

    public S3Gateway(S3GatewayConfig config, CandyboxClient client) {
        this.server = new S3GatewayServer(config, new CandyboxClientStore(client));
    }

    /** Binds and starts serving. */
    public void start() {
        server.start();
    }

    /** The actual bound port (useful when configured with port 0). */
    public int port() {
        return server.port();
    }

    @Override
    public void close() {
        server.close();
    }
}
