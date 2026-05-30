package me.predatorray.candybox.server;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.engine.CandyMetadata;
import me.predatorray.candybox.lsm.engine.ListResult;
import me.predatorray.candybox.lsm.engine.ScanDirection;
import me.predatorray.candybox.lsm.engine.ScanQuery;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates protocol {@link Frame}s into {@link BoxEngine} calls on a {@link CandyboxNode} and maps
 * results (and the Candybox exception hierarchy) back into response frames — including the retriable
 * {@code BUSY} response under write-stall.
 *
 * <p>Candy CRUD and list, plus createBox/deleteBox, are fully wired. listBoxes/headBox and large-object
 * streaming are TODO(phase-2).
 */
final class NodeRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NodeRequestHandler.class);

    private final CandyboxNode node;
    private final MessageCodec codec = new MessageCodec();

    NodeRequestHandler(CandyboxNode node) {
        this.node = node;
    }

    @Override
    public Frame handle(Frame request) {
        Message message;
        try {
            message = codec.decode(request);
        } catch (RuntimeException e) {
            return codec.encode(new Message.ErrorResponse("ProtocolError", safe(e.getMessage())));
        }
        try {
            return codec.encode(dispatch(message));
        } catch (BusyException e) {
            return codec.encode(new Message.BusyResponse(100));
        } catch (CandyNotFoundException e) {
            return codec.encode(new Message.NotFoundResponse());
        } catch (NotOwnerException | BoxNotFoundException e) {
            // We don't own this Box. If another node does, tell the client to re-route; else not found.
            return codec.encode(movedOrNotFound(message));
        } catch (CandyboxException e) {
            return codec.encode(new Message.ErrorResponse(e.getClass().getSimpleName(), safe(e.getMessage())));
        } catch (RuntimeException e) {
            LOG.warn("Unexpected error handling request", e);
            return codec.encode(new Message.ErrorResponse("InternalError", safe(e.getMessage())));
        }
    }

    private Message movedOrNotFound(Message message) {
        String box = boxOf(message);
        if (box != null) {
            Optional<Integer> owner = node.currentOwner(BoxName.of(box));
            if (owner.isPresent() && owner.get() != node.nodeId()) {
                return new Message.MovedResponse(owner.get());
            }
        }
        return new Message.NotFoundResponse();
    }

    /** The target Box of a Box-routed request, or {@code null} for cluster-wide requests. */
    private static String boxOf(Message message) {
        if (message instanceof Message.PutCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.GetCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.HeadCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.DeleteCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.ListCandiesRequest m) {
            return m.box();
        } else if (message instanceof Message.DeleteBoxRequest m) {
            return m.box();
        } else if (message instanceof Message.HeadBoxRequest m) {
            return m.box();
        }
        return null;
    }

    private Message dispatch(Message message) {
        if (message instanceof Message.CreateBoxRequest m) {
            node.createBox(BoxName.of(m.box()));
            return new Message.OkResponse();
        } else if (message instanceof Message.DeleteBoxRequest m) {
            node.deleteBox(BoxName.of(m.box()), m.force());
            return new Message.OkResponse();
        } else if (message instanceof Message.PutCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()));
            engine.putCandy(CandyKey.of(m.key()), m.data(), m.contentType(), m.userMetadata(),
                    m.idempotencyToken());
            return new Message.OkResponse();
        } else if (message instanceof Message.GetCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CandyMetadata meta = engine.getCandy(CandyKey.of(m.key()), out);
            return new Message.CandyDataResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), out.toByteArray());
        } else if (message instanceof Message.HeadCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()));
            CandyMetadata meta = engine.headCandy(CandyKey.of(m.key()));
            return new Message.HeadCandyResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), meta.createdAtMillis());
        } else if (message instanceof Message.DeleteCandyRequest m) {
            node.engine(BoxName.of(m.box())).deleteCandy(CandyKey.of(m.key()));
            return new Message.OkResponse();
        } else if (message instanceof Message.ListCandiesRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()));
            ListResult result = engine.scanCandies(toScanQuery(m));
            List<Message.ListedCandy> entries = new ArrayList<>();
            for (ListResult.ListEntry e : result.entries()) {
                entries.add(new Message.ListedCandy(e.key().value(), e.contentLength(),
                        e.createdAtMillis()));
            }
            return new Message.ListCandiesResponse(entries, result.nextStartAfter());
        } else if (message instanceof Message.ListBoxesRequest) {
            // Boxes owned by this node. Cluster-wide listing needs a coordination list op (later).
            return new Message.ListBoxesResponse(node.listBoxes());
        } else if (message instanceof Message.HeadBoxRequest m) {
            return node.boxExists(BoxName.of(m.box()))
                    ? new Message.OkResponse()
                    : new Message.NotFoundResponse();
        }
        return new Message.ErrorResponse("UnsupportedOperation",
                "Not implemented in this phase: " + message.opcode());
    }

    private static ScanQuery toScanQuery(Message.ListCandiesRequest m) {
        CandyKey start = m.startKey() == null ? null : CandyKey.of(m.startKey());
        CandyKey end = m.endKey() == null ? null : CandyKey.of(m.endKey());
        CandyKey cursor = m.startAfter() == null ? null : CandyKey.of(m.startAfter());
        ScanDirection direction = m.reverse() ? ScanDirection.REVERSE : ScanDirection.FORWARD;
        return new ScanQuery(m.prefix(), start, end, cursor, direction, m.maxKeys());
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }
}
