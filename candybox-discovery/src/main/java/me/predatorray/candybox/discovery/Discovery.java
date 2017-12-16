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

import java.io.Closeable;

/**
 * The root interface for a discovery implementation.
 *
 * <p>Discovery is a mechanism in CandyBox by which any nodes in a cluster are able to find out the necessary
 * information needed to connect to a node that hosts certain RPC service.
 * <p>Those RPC services are organized according to their functions. All service endpoints that host the services
 * having the same function advertise their connection information using the same identifier, which is called
 * a service group name.
 *
 * <p>In an invoker's point of view, an instance of this class provides the ability to get a Service Group by its name,
 * and further discovers available endpoints hosting RPC services.
 *
 * <p>In an implementor's point of view, all the interfaces like {@link ServiceGroup} and
 * {@link Endpoint} along with the {@code Discovery} should be implemented accordingly. And the API requests
 * should be forwarded to the actual data structure or remote services behind that finally store and serve the endpoint
 * information.
 *
 * @author Wenhao Ji
 */
public interface Discovery extends Closeable {

    /**
     * Retrieve the Service Group by its name or create one if not exists.
     *
     * <p>A Discovery implementation may have restriction on the naming of a service group. If a ServiceGroup cannot
     * be created using the provided name, a {@link IllegalArgumentException} should be thrown.
     *
     * <p>This method MUST never return {@code null}. A {@link DiscoveryException} should be thrown if the ServiceGroup
     * cannot be created or resolved because of some other exception occurred.
     *
     * @param serviceGroupName the name of the service group
     * @return a non-null ServiceGroup instance
     * @throws IllegalArgumentException if the serviceGroupName doesn't obey the naming convention of the implementation
     * @throws DiscoveryException if the group cannot be created or resolved by its name because of any other errors
     */
    ServiceGroup getServiceGroup(String serviceGroupName) throws IllegalArgumentException, DiscoveryException;
}
