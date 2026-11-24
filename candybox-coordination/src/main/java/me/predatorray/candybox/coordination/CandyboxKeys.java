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

    /** Pointer namespace isolated from every other incarnation of the same Box name. */
    public static String incarnationManifestKey(String boxName, int partition, String incarnation) {
        if (incarnation == null || incarnation.isBlank() || incarnation.contains("/")) {
            throw new IllegalArgumentException("Invalid Box incarnation");
        }
        return manifestKey(boxName, partition) + "/incarnations/" + incarnation;
    }

    /** The versioned key holding a Box's ACL document (owner + grants). */
    public static String boxAclKey(String boxName) {
        return "acls/" + boxName;
    }

    /**
     * The versioned key holding one partition's published held-Syrup id set (referenced, or released
     * less than a GC grace period ago), consulted by the Box-global garbage collector so a Syrup
     * shared cross-partition (zero-copy copy/rename) is never reclaimed while any partition still
     * points at it, nor sooner than a grace period after the last one let go.
     */
    public static String partitionRefsKey(String boxName, int partition) {
        return BOXES_ROOT + "/" + boxName + "/partitions/" + partition + "/refs";
    }

    /**
     * The rendezvous marker a destination owner writes when a cross-partition rename's zero-copy put
     * is durable; the source owner reads it to finalize (tombstone the source) the rename.
     */
    public static String renameMarkerKey(String boxName, String renameToken) {
        return BOXES_ROOT + "/" + boxName + "/renames/" + renameToken;
    }

    /**
     * The directory of locator relays a partition's owner stashed for cross-partition zero-copy
     * puts; the owner reaps stale ones.
     */
    public static String locatorRelaysPath(String boxName, int partition) {
        return BOXES_ROOT + "/" + boxName + "/partitions/" + partition + "/relays";
    }

    /**
     * A source locator snapshot written by its partition's owner and read by the destination owner of
     * a cross-partition zero-copy put, so the parts and ACL it writes never pass through the client.
     */
    public static String locatorRelayKey(String boxName, int partition, String relayToken) {
        return locatorRelaysPath(boxName, partition) + "/" + relayToken;
    }
}
