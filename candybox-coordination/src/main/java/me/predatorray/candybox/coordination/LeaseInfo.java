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
