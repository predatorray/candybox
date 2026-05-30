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

import me.predatorray.candybox.protocol.Message;

/**
 * Sends a request message to the right node and returns the decoded response. Two implementations:
 * {@link DirectRouter} (a fixed single node, for single-node use and tests) and {@link ClusterRouter}
 * (resolves the Box's owner from coordination and re-routes on {@code MOVED}).
 */
interface Router extends AutoCloseable {

    /** Routes a Box-scoped request to that Box's current owner. */
    Message callBox(String box, Message request);

    /** Routes a cluster-wide request (createBox, listBoxes) to any reachable node. */
    Message callAny(Message request);

    @Override
    void close();
}
