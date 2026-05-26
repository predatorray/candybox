package me.predatorray.candybox.protocol.transport;

import me.predatorray.candybox.protocol.Frame;

/** A client-side connection that performs synchronous request/response RPCs. */
public interface Connection extends AutoCloseable {

    /**
     * Sends a request and returns the response (blocking).
     *
     * @param request the request frame
     * @return the response frame
     */
    Frame call(Frame request);

    @Override
    void close();
}
