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
package me.predatorray.candybox.common.auth;

/**
 * What an {@link Operation} is checked against: the cluster itself (create Box, list Boxes) or one
 * Box. Object-level grants live inside the {@code CandyLocator} and are unioned with the Box grant
 * by the read path, so objects are not separate authorizer resources.
 */
public record Resource(Type type, String name) {

    public enum Type {
        CLUSTER, BOX
    }

    public static final Resource CLUSTER = new Resource(Type.CLUSTER, "");

    public static Resource box(String name) {
        return new Resource(Type.BOX, name);
    }

    @Override
    public String toString() {
        return type == Type.CLUSTER ? "Cluster" : "Box:" + name;
    }
}
