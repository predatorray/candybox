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

import java.net.URI;

import static org.junit.Assert.*;

public class SimpleEndpointTest {

    @Test
    public void identifiersAreBoughtBack() throws Exception {
        String instanceId = "foobar";
        URI location = URI.create("foobar://127.0.0.1:8080");
        SimpleEndpoint endpoint = new SimpleEndpoint(instanceId, location);
        assertEquals(instanceId, endpoint.instanceIdentifier());
        assertEquals(location, endpoint.locationIdentifier());
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullInstanceIdentifierIsNotAllowed() throws Exception {
        URI location = URI.create("foobar://127.0.0.1:8080");
        new SimpleEndpoint(null, location);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullLocationIdentifierIsNotAllowed() throws Exception {
        String instanceId = "foobar";
        new SimpleEndpoint(instanceId, null);
    }
}