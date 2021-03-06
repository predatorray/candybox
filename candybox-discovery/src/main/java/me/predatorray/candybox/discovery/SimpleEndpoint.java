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

import me.predatorray.candybox.util.Validations;

import java.net.URI;
import java.util.Objects;

public class SimpleEndpoint implements Endpoint {

    private final String instanceIdentifier;
    private final URI locationIdentifier;

    public SimpleEndpoint(String instanceIdentifier, URI locationIdentifier) {
        this.instanceIdentifier = Validations.notNull(instanceIdentifier);
        this.locationIdentifier = Validations.notNull(locationIdentifier);
    }

    @Override
    public String instanceIdentifier() {
        return instanceIdentifier;
    }

    @Override
    public URI locationIdentifier() {
        return locationIdentifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleEndpoint that = (SimpleEndpoint) o;
        return Objects.equals(instanceIdentifier, that.instanceIdentifier) &&
                Objects.equals(locationIdentifier, that.locationIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceIdentifier, locationIdentifier);
    }

    @Override
    public String toString() {
        return "SimpleEndpoint {" +
                "instanceIdentifier='" + instanceIdentifier + '\'' +
                ", locationIdentifier=" + locationIdentifier +
                '}';
    }
}
