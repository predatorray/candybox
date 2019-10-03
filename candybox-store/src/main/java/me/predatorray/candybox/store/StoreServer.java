/*
 * Copyright (c) 2018 the original author or authors.
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

package me.predatorray.candybox.store;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import me.predatorray.candybox.store.config.Configuration;
import me.predatorray.candybox.store.config.DefaultConfiguration;
import me.predatorray.candybox.store.service.ShardService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class StoreServer {

    private final Server server;

    public StoreServer(int port, ShardService shardService) {
        this.server = ServerBuilder.forPort(port)
            .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
            .addService(shardService)
            .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void stop() {
        server.shutdown();
    }

    public void blockUntilShutdown() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        try (Configuration configuration = new DefaultConfiguration()) {
            List<Path> dataPaths = configuration.getDataDirectoryPaths();
            if (dataPaths.size() != 1) {
                throw new IllegalArgumentException("Only one data directory is supported by now.");
            }

            try (SingleDataDirLocalShardManager localShardManager = new SingleDataDirLocalShardManager(configuration)) {
                ShardService shardService = new ShardService(localShardManager);
                StoreServer storeServer = new StoreServer(configuration.getRpcPort(), shardService);
                storeServer.start();

                Runtime.getRuntime().addShutdownHook(new Thread(storeServer::stop));
                storeServer.blockUntilShutdown();
            }
        }
    }
}
