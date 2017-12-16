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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArraySet;

public class InMemoryServiceGroup implements ServiceGroup {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryServiceGroup.class);

    private final String serviceGroupName;
    private final CopyOnWriteArraySet<Endpoint> endpoints;

    public InMemoryServiceGroup(String serviceGroupName) {
        this.serviceGroupName = serviceGroupName;
        this.endpoints = new CopyOnWriteArraySet<>();
    }

    @Override
    public Collection<Endpoint> discoverAllAvailableEndpoints() {
        return new ArrayList<>(endpoints);
    }

    @Override
    public Optional<Endpoint> discoverAny() {
        return endpoints.stream().findFirst();
    }

    @Override
    public void advertise(Endpoint endpoint) {
        if (endpoints.add(endpoint)) {
            logger.debug("The endpoint {} is advertised.", endpoint);
        } else {
            logger.debug("The endpoint {} has already been advertised.", endpoint);
        }
    }

    @Override
    public void conceal(Endpoint endpoint) {
        if (endpoints.remove(endpoint)) {
            logger.debug("The endpoint {} is concealed.", endpoint);
        } else {
            logger.debug("The endpoint {} has never been advertised or already been concealed.", endpoint);
        }
    }

    @Override
    public String toString() {
        return "InMemoryServiceGroup {" +
                "serviceGroupName='" + serviceGroupName + '\'' +
                '}';
    }
}
