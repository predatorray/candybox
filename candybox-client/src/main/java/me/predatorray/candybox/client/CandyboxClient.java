package me.predatorray.candybox.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.Validation;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.common.exception.StorageException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.transport.Transport;

/**
 * The thin client library: exposes the public Candybox API over the {@link Transport} SPI. It builds
 * typed {@link Message}s, hands them to a {@link Router}, and maps responses back to results or the
 * Candybox exception hierarchy. Client-side size validation fails fast before a request is sent.
 *
 * <p>Construct with a {@code host:port} for a single node ({@link DirectRouter}), or with a
 * {@link CoordinationService} for a cluster ({@link ClusterRouter}, which resolves each Box's owner and
 * re-routes on {@code MOVED}). Large objects are buffered in memory for now; chunked streaming over the
 * wire is TODO(phase-2.5).
 */
public final class CandyboxClient implements AutoCloseable {

    private final Router router;
    private final SizeLimits limits;

    /** Single-node client talking directly to {@code host:port}. */
    public CandyboxClient(Transport transport, String host, int port) {
        this(transport, host, port, SizeLimits.defaults());
    }

    /** Single-node client with explicit size limits. */
    public CandyboxClient(Transport transport, String host, int port, SizeLimits limits) {
        this.router = new DirectRouter(transport, host, port);
        this.limits = limits;
    }

    /** Cluster-aware client: routes each request to the owning node via coordination. */
    public CandyboxClient(Transport transport, CoordinationService coordination, CandyboxConfig config) {
        this.router = new ClusterRouter(transport, coordination, config.routerCacheTtlMillis(),
                SystemClock.INSTANCE);
        this.limits = config.sizeLimits();
    }

    // ---- Box admin -------------------------------------------------------------------------

    public void createBox(String box) {
        expectOk(router.callAny(new Message.CreateBoxRequest(BoxName.of(box).value())));
    }

    public void deleteBox(String box, boolean force) {
        expectOk(router.callBox(box, new Message.DeleteBoxRequest(BoxName.of(box).value(), force)));
    }

    // ---- Candy ops -------------------------------------------------------------------------

    public void putCandy(String box, String key, byte[] data, String contentType,
                         Map<String, String> userMetadata, String idempotencyToken) {
        CandyKey candyKey = CandyKey.of(key);
        Validation.checkCandyKey(candyKey, limits);
        Validation.checkUserMetadata(userMetadata, limits);
        Validation.checkCandySize(data.length, limits);
        expectOk(router.callBox(box, new Message.PutCandyRequest(BoxName.of(box).value(),
                candyKey.value(), contentType, userMetadata == null ? Map.of() : userMetadata,
                idempotencyToken, data)));
    }

    /** Streaming put (buffers the stream in memory for now — TODO(phase-2): true streaming). */
    public void putCandy(String box, String key, InputStream data, String contentType,
                         Map<String, String> userMetadata, String idempotencyToken) {
        putCandy(box, key, readFully(data), contentType, userMetadata, idempotencyToken);
    }

    public byte[] getCandy(String box, String key) {
        Message response = router.callBox(box, new Message.GetCandyRequest(BoxName.of(box).value(),
                CandyKey.of(key).value()));
        if (response instanceof Message.CandyDataResponse data) {
            return data.data();
        }
        throw mapUnexpected(response, box, key);
    }

    /** Streaming get convenience: writes the bytes to {@code out}. */
    public void getCandy(String box, String key, OutputStream out) {
        byte[] data = getCandy(box, key);
        try {
            out.write(data);
        } catch (IOException e) {
            throw new StorageException("Failed writing Candy to output stream", e);
        }
    }

    public CandyInfo headCandy(String box, String key) {
        Message response = router.callBox(box, new Message.HeadCandyRequest(BoxName.of(box).value(),
                CandyKey.of(key).value()));
        if (response instanceof Message.HeadCandyResponse head) {
            return new CandyInfo(head.contentLength(), head.contentType(), head.userMetadata(),
                    head.crc32c(), head.createdAtMillis());
        }
        throw mapUnexpected(response, box, key);
    }

    /** Lists the Boxes known to the contacted node. (Cluster-wide listing is a later refinement.) */
    public List<String> listBoxes() {
        Message response = router.callAny(new Message.ListBoxesRequest());
        if (response instanceof Message.ListBoxesResponse boxes) {
            return boxes.boxes();
        }
        throw mapResponse(response);
    }

    /** Returns whether the Box exists (has a current owner). */
    public boolean headBox(String box) {
        Message response;
        try {
            response = router.callBox(box, new Message.HeadBoxRequest(BoxName.of(box).value()));
        } catch (NotOwnerException noOwner) {
            return false; // no current owner ⇒ not currently a live Box
        }
        if (response instanceof Message.OkResponse) {
            return true;
        }
        if (response instanceof Message.NotFoundResponse) {
            return false;
        }
        throw mapResponse(response);
    }

    public void deleteCandy(String box, String key) {
        expectOk(router.callBox(box,
                new Message.DeleteCandyRequest(BoxName.of(box).value(), CandyKey.of(key).value())));
    }

    /**
     * Zero-copy server-side copy of {@code srcKey} to {@code dstKey} within the same Box: the new key
     * reuses the source's stored bytes (no data is transferred). Returns the destination's metadata.
     */
    public CandyInfo copyCandy(String box, String srcKey, String dstKey, String idempotencyToken) {
        return copyOrRename(box, srcKey, new Message.CopyCandyRequest(BoxName.of(box).value(),
                CandyKey.of(srcKey).value(), CandyKey.of(dstKey).value(), idempotencyToken));
    }

    /**
     * Zero-copy server-side rename/move of {@code srcKey} to {@code dstKey} within the same Box: the
     * bytes never move; the source key is atomically removed. Returns the destination's metadata.
     */
    public CandyInfo renameCandy(String box, String srcKey, String dstKey, String idempotencyToken) {
        return copyOrRename(box, srcKey, new Message.RenameCandyRequest(BoxName.of(box).value(),
                CandyKey.of(srcKey).value(), CandyKey.of(dstKey).value(), idempotencyToken));
    }

    private CandyInfo copyOrRename(String box, String srcKey, Message request) {
        Message response = router.callBox(box, request);
        if (response instanceof Message.HeadCandyResponse head) {
            return new CandyInfo(head.contentLength(), head.contentType(), head.userMetadata(),
                    head.crc32c(), head.createdAtMillis());
        }
        throw mapUnexpected(response, box, srcKey);
    }

    /**
     * Deletes every Candy whose key starts with {@code prefix} using a single server-side range
     * tombstone (O(1), not a list-then-delete). An empty/null prefix deletes the whole Box's contents.
     */
    public void deleteRangeByPrefix(String box, String prefix) {
        expectOk(router.callBox(box,
                new Message.DeleteRangeRequest(BoxName.of(box).value(), prefix == null ? "" : prefix,
                        null, null)));
    }

    /**
     * Deletes every Candy whose key falls in {@code [startKey, endKey)} (either bound nullable) using a
     * single server-side range tombstone.
     */
    public void deleteRange(String box, String startKey, String endKey) {
        expectOk(router.callBox(box,
                new Message.DeleteRangeRequest(BoxName.of(box).value(), null, startKey, endKey)));
    }

    public Listing listCandies(String box, String prefix, String startAfter, int maxKeys) {
        return listCandies(box, new Message.ListCandiesRequest(BoxName.of(box).value(), prefix,
                startAfter, maxKeys));
    }

    /**
     * Range/directional listing: lists live Candies over the half-open window {@code [startKey, endKey)}
     * (either bound nullable), optionally narrowed by {@code prefix}, walked forward or in reverse, and
     * paged via {@code startAfter} (the previous page's {@code nextStartAfter}, exclusive in the scan
     * direction).
     */
    public Listing listCandies(String box, String prefix, String startKey, String endKey,
                               String startAfter, boolean reverse, int maxKeys) {
        return listCandies(box, new Message.ListCandiesRequest(BoxName.of(box).value(), prefix,
                startAfter, maxKeys, startKey, endKey, reverse));
    }

    private Listing listCandies(String box, Message.ListCandiesRequest request) {
        Message response = router.callBox(box, request);
        if (response instanceof Message.ListCandiesResponse list) {
            List<Listing.Entry> entries = new ArrayList<>();
            for (Message.ListedCandy c : list.entries()) {
                entries.add(new Listing.Entry(c.key(), c.contentLength(), c.createdAtMillis()));
            }
            return new Listing(entries, list.nextStartAfter());
        }
        throw mapResponse(response);
    }

    @Override
    public void close() {
        router.close();
    }

    // ---- internals -------------------------------------------------------------------------

    private void expectOk(Message response) {
        if (!(response instanceof Message.OkResponse)) {
            throw mapResponse(response);
        }
    }

    private CandyboxException mapResponse(Message response) {
        if (response instanceof Message.BusyResponse busy) {
            return new BusyException("Server busy; retry after " + busy.retryAfterMillis() + "ms");
        }
        if (response instanceof Message.ErrorResponse err) {
            return new CandyboxException(err.errorType() + ": " + err.message());
        }
        if (response instanceof Message.NotFoundResponse) {
            return new CandyboxException("Not found");
        }
        if (response instanceof Message.MovedResponse moved) {
            // TODO(phase-2 WS5): re-route to moved.ownerNodeId() via the ClusterRouter and retry.
            return new NotOwnerException("owned by node " + moved.ownerNodeId());
        }
        return new CandyboxException("Unexpected response: " + response.opcode());
    }

    private CandyboxException mapUnexpected(Message response, String box, String key) {
        if (response instanceof Message.NotFoundResponse) {
            return new CandyNotFoundException(box, key);
        }
        return mapResponse(response);
    }

    private static byte[] readFully(InputStream in) {
        try {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new StorageException("Failed reading input stream", e);
        }
    }

    /** Metadata returned by {@code headCandy}. */
    public record CandyInfo(long contentLength, String contentType, Map<String, String> userMetadata,
                            int crc32c, long createdAtMillis) {
    }

    /** A page of {@code listCandies} results. */
    public record Listing(List<Entry> entries, String nextStartAfter) {
        public boolean isTruncated() {
            return nextStartAfter != null;
        }

        public record Entry(String key, long contentLength, long createdAtMillis) {
        }
    }
}
