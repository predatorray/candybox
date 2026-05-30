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
package me.predatorray.candybox.coordination;

/**
 * A read-only view of the current holder of a lease, returned by
 * {@link CoordinationService#leaseHolder(String)}. Used for routing: a node that does not own a Box
 * looks up who does, and a client resolves a Box to its owning node.
 *
 * @param ownerNodeId   the node currently holding the lease
 * @param fencingToken  that holder's fencing token
 */
public record LeaseInfo(int ownerNodeId, long fencingToken) {
}
