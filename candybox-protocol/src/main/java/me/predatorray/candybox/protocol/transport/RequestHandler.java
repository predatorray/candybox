package me.predatorray.candybox.protocol.transport;

import me.predatorray.candybox.protocol.Frame;

/** Server-side request handler: maps a request frame to a response frame. */
@FunctionalInterface
public interface RequestHandler {

    /**
     * Handles one request.
     *
     * @param request the decoded request frame
     * @return the response frame
     */
    Frame handle(Frame request);
}
