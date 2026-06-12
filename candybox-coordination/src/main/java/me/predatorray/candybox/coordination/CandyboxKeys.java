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
 * Canonical coordination key/resource names, shared by the server (which owns Box partitions) and
 * the client (which routes to owners) so both agree on where the Box descriptor, the per-partition
 * ownership lease and manifest pointer, and the cluster-wide balancer state live.
 */
public final class CandyboxKeys {

    /** The root under which every Box's coordination state lives. */
    public static final String BOXES_ROOT = "boxes";

    /** The coordinator-election lease for the partition balancer. */
    public static final String BALANCER_RESOURCE = "cluster/balancer";

    /** The versioned key holding the desired partition→node assignment table. */
    public static final String ASSIGNMENT_KEY = "cluster/assignment";

    private CandyboxKeys() {
    }

    /** The versioned key holding a Box's immutable descriptor (partition count). */
    public static String boxMetaKey(String boxName) {
        return BOXES_ROOT + "/" + boxName + "/meta";
    }

    /** The ownership-lease resource for one partition of a Box. The lease holder is its owner. */
    public static String ownerResource(String boxName, int partition) {
        return BOXES_ROOT + "/" + boxName + "/partitions/" + partition + "/owner";
    }

    /** The versioned key holding the pointer to one partition's current manifest ledger. */
    public static String manifestKey(String boxName, int partition) {
        return BOXES_ROOT + "/" + boxName + "/partitions/" + partition + "/manifest";
    }
}
