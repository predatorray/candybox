package me.predatorray.candybox.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Validation;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.common.exception.StorageException;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;

/**
 * The thin client library: exposes the public Candybox API over the {@link Transport} SPI. It builds
 * typed {@link Message}s, sends them over a {@link Connection}, and maps responses back to results or
 * the Candybox exception hierarchy. Client-side size validation fails fast before a request is sent.
 *
 * <p>v1 talks to a single node. Multi-node routing via Box assignment (Phase 2) layers on top of this
 * by selecting the connection per request. Large objects are buffered in memory for now; chunked
 * streaming over the wire is TODO(phase-2).
 */
public final class CandyboxClient implements AutoCloseable {

    private final Connection connection;
    private final MessageCodec codec = new MessageCodec();
    private final SizeLimits limits;

    public CandyboxClient(Transport transport, String host, int port) {
        this(transport, host, port, SizeLimits.defaults());
    }

    public CandyboxClient(Transport transport, String host, int port, SizeLimits limits) {
        this.connection = transport.connect(host, port);
        this.limits = limits;
    }

    // ---- Box admin -------------------------------------------------------------------------

    public void createBox(String box) {
        expectOk(call(new Message.CreateBoxRequest(BoxName.of(box).value())));
    }

    public void deleteBox(String box, boolean force) {
        expectOk(call(new Message.DeleteBoxRequest(BoxName.of(box).value(), force)));
    }

    // ---- Candy ops -------------------------------------------------------------------------

    public void putCandy(String box, String key, byte[] data, String contentType,
                         Map<String, String> userMetadata, String idempotencyToken) {
        CandyKey candyKey = CandyKey.of(key);
        Validation.checkCandyKey(candyKey, limits);
        Validation.checkUserMetadata(userMetadata, limits);
        Validation.checkCandySize(data.length, limits);
        expectOk(call(new Message.PutCandyRequest(BoxName.of(box).value(), candyKey.value(),
                contentType, userMetadata == null ? Map.of() : userMetadata, idempotencyToken, data)));
    }

    /** Streaming put (buffers the stream in memory for now — TODO(phase-2): true streaming). */
    public void putCandy(String box, String key, InputStream data, String contentType,
                         Map<String, String> userMetadata, String idempotencyToken) {
        putCandy(box, key, readFully(data), contentType, userMetadata, idempotencyToken);
    }

    public byte[] getCandy(String box, String key) {
        Message response = call(new Message.GetCandyRequest(BoxName.of(box).value(),
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
        Message response = call(new Message.HeadCandyRequest(BoxName.of(box).value(),
                CandyKey.of(key).value()));
        if (response instanceof Message.HeadCandyResponse head) {
            return new CandyInfo(head.contentLength(), head.contentType(), head.userMetadata(),
                    head.crc32c(), head.createdAtMillis());
        }
        throw mapUnexpected(response, box, key);
    }

    /** Lists the Boxes known to the contacted node. */
    public List<String> listBoxes() {
        Message response = call(new Message.ListBoxesRequest());
        if (response instanceof Message.ListBoxesResponse boxes) {
            return boxes.boxes();
        }
        throw mapResponse(response);
    }

    /** Returns whether the Box exists (is owned by the contacted node). */
    public boolean headBox(String box) {
        Message response = call(new Message.HeadBoxRequest(BoxName.of(box).value()));
        if (response instanceof Message.OkResponse) {
            return true;
        }
        if (response instanceof Message.NotFoundResponse) {
            return false;
        }
        throw mapResponse(response);
    }

    public void deleteCandy(String box, String key) {
        expectOk(call(new Message.DeleteCandyRequest(BoxName.of(box).value(), CandyKey.of(key).value())));
    }

    public Listing listCandies(String box, String prefix, String startAfter, int maxKeys) {
        Message response = call(new Message.ListCandiesRequest(BoxName.of(box).value(), prefix,
                startAfter, maxKeys));
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
        connection.close();
    }

    // ---- internals -------------------------------------------------------------------------

    private Message call(Message request) {
        Frame responseFrame = connection.call(codec.encode(request));
        return codec.decode(responseFrame);
    }

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
