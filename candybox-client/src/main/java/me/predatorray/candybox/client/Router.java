package me.predatorray.candybox.client;

import me.predatorray.candybox.protocol.Message;

/**
 * Sends a request message to the right node and returns the decoded response. Two implementations:
 * {@link DirectRouter} (a fixed single node, for single-node use and tests) and {@link ClusterRouter}
 * (resolves the Box's owner from coordination and re-routes on {@code MOVED}).
 */
interface Router extends AutoCloseable {

    /** Routes a Box-scoped request to that Box's current owner. */
    Message callBox(String box, Message request);

    /** Routes a cluster-wide request (createBox, listBoxes) to any reachable node. */
    Message callAny(Message request);

    @Override
    void close();
}
