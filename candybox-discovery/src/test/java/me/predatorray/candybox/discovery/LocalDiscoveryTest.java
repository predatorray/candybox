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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class LocalDiscoveryTest {

    @Test
    public void localDiscoveryUsesInMemoryServiceGroup() throws Exception {
        try (LocalDiscovery localDiscovery = new LocalDiscovery()) {
            ServiceGroup serviceGroup = localDiscovery.getServiceGroup("srv1");
            assertTrue(serviceGroup instanceof InMemoryServiceGroup);
        }
    }

    @Test
    public void twoServiceGroupsOfTheSameNameActIdentically() throws Exception {
        final String serviceGroupName = "srv1";
        final Endpoint endpoint = mock(Endpoint.class);
        try (LocalDiscovery localDiscovery = new LocalDiscovery()) {
            ServiceGroup[] identicalGroups = new ServiceGroup[] {
                    localDiscovery.getServiceGroup(serviceGroupName),
                    localDiscovery.getServiceGroup(serviceGroupName)
            };
            identicalGroups[0].advertise(endpoint);

            Assert.assertEquals(identicalGroups[0].discoverAllAvailableEndpoints(),
                    identicalGroups[1].discoverAllAvailableEndpoints());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void invokeMethodOnAClosedInstanceIsNotLegal() throws Exception {
        LocalDiscovery localDiscovery = new LocalDiscovery();
        localDiscovery.close();

        localDiscovery.getServiceGroup("any");
    }
}
