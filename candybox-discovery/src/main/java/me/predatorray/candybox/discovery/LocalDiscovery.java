/*
 * Copyright (c) 2017 the original author or authors.
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

package me.predatorray.candybox.discovery;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A local implementation of Discovery which stores service groups and their endpoints in memory.
 *
 * <p>A typical use case of this class is to avoid unnecessary network connections finding a service group which
 * resides on the same JVM.</p>
 *
 * @author Wenhao Ji
 */
public class LocalDiscovery extends AbstractDiscovery {

    private final ConcurrentMap<String, ServiceGroup> serviceGroups = new ConcurrentHashMap<>();

    @Override
    public ServiceGroup getServiceGroup(String serviceGroupName) {
        ensureNotClosed();
        return serviceGroups.computeIfAbsent(serviceGroupName, InMemoryServiceGroup::new);
    }

    @Override
    public void close() throws IOException {
        super.close();
        serviceGroups.clear();
    }
}
