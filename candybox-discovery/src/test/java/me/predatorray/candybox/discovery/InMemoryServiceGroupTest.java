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

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class InMemoryServiceGroupTest {

    @Test
    public void noEndpointsAreDiscoveredIfNewlyCreated() throws Exception {
        InMemoryServiceGroup newServiceGroup = new InMemoryServiceGroup("srv1");

        Collection<Endpoint> allAvailableEndpoints = newServiceGroup.discoverAllAvailableEndpoints();
        assertNotNull(allAvailableEndpoints);
        assertTrue(allAvailableEndpoints.isEmpty());

        Optional<Endpoint> anyEndpoint = newServiceGroup.discoverAny();
        assertFalse(anyEndpoint.isPresent());
    }

    @Test
    public void sameEndpointIsDiscoveredAfterAdvertised() throws Exception {
        InMemoryServiceGroup newServiceGroup = new InMemoryServiceGroup("srv1");
        Endpoint endpoint = mock(Endpoint.class);
        newServiceGroup.advertise(endpoint);

        Collection<Endpoint> endpoints = newServiceGroup.discoverAllAvailableEndpoints();
        assertEquals(Collections.singletonList(endpoint), endpoints);

        Optional<Endpoint> anyEndpoint = newServiceGroup.discoverAny();
        assertTrue(anyEndpoint.isPresent());
        assertSame(endpoint, anyEndpoint.get());
    }

    @Test
    public void advertisedEndpointIsNotDiscoveredAfterConcealed() throws Exception {
        InMemoryServiceGroup newServiceGroup = new InMemoryServiceGroup("srv1");
        Endpoint endpoint = mock(Endpoint.class);
        newServiceGroup.advertise(endpoint);
        newServiceGroup.conceal(endpoint);

        assertTrue(newServiceGroup.discoverAllAvailableEndpoints().isEmpty());
        assertFalse(newServiceGroup.discoverAny().isPresent());
    }

    @Test
    public void duplicateAdvertisementsAreIgnored() throws Exception {
        InMemoryServiceGroup newServiceGroup = new InMemoryServiceGroup("srv1");
        Endpoint endpoint = mock(Endpoint.class);
        final int timesAdvertised = 3;
        for (int i = 0; i < timesAdvertised; i++) {
            newServiceGroup.advertise(endpoint);
        }

        assertEquals(1, newServiceGroup.discoverAllAvailableEndpoints().size());
    }

    @Test
    public void duplicateConcealmentIsIgnored() throws Exception {
        InMemoryServiceGroup newServiceGroup = new InMemoryServiceGroup("srv1");
        Endpoint endpoint = mock(Endpoint.class);
        newServiceGroup.advertise(endpoint);

        final int timesAdvertised = 3;
        for (int i = 0; i < timesAdvertised; i++) {
            newServiceGroup.conceal(endpoint);
        }

        assertTrue(newServiceGroup.discoverAllAvailableEndpoints().isEmpty());
    }

    @Test
    public void concealingNonExistedGroupIsOk() throws Exception {
        InMemoryServiceGroup newServiceGroup = new InMemoryServiceGroup("srv1");
        Endpoint endpoint = mock(Endpoint.class);
        newServiceGroup.conceal(endpoint);

        assertTrue(newServiceGroup.discoverAllAvailableEndpoints().isEmpty());
    }
}
