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
package me.predatorray.candybox.server;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.auth.Grant;
import me.predatorray.candybox.common.auth.Operation;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.auth.Resource;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.common.exception.ValidationException;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.engine.CandyMetadata;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.lsm.engine.ListResult;
import me.predatorray.candybox.lsm.engine.ScanDirection;
import me.predatorray.candybox.lsm.engine.ScanQuery;
import me.predatorray.candybox.lsm.manifest.MultipartUploadState;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.ConnectionContext;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates protocol {@link Frame}s into {@link BoxEngine} calls on a {@link CandyboxNode} and maps
 * results (and the Candybox exception hierarchy) back into response frames — including the retriable
 * {@code BUSY} response under write-stall.
 *
 * <p>Every keyed request is dispatched to the engine of the key's hash partition; partition-scoped
 * requests (list, delete-range, list-uploads — fanned out by the client) carry an explicit partition.
 * A request landing on a node that does not own the target partition gets a {@code MOVED} response
 * naming the partition's current owner.
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
        return handle(new ConnectionContext(), request);
    }

    @Override
    public Frame handle(ConnectionContext context, Frame request) {
        Message message;
        try {
            message = codec.decode(request);
        } catch (RuntimeException e) {
            return codec.encode(new Message.ErrorResponse("ProtocolError", safe(e.getMessage())));
        }
        Principal principal = context.principalOrAnonymous();
        try {
            Access access = requiredAccess(message);
            if (access != null
                    && !node.authorizer().authorize(principal, access.operation(), access.resource())) {
                return codec.encode(new Message.AccessDeniedResponse(
                        principal + " is not allowed " + access.operation() + " on "
                                + access.resource()));
            }
            return codec.encode(dispatch(message, principal));
        } catch (BusyException e) {
            return codec.encode(new Message.BusyResponse(100));
        } catch (CandyNotFoundException e) {
            return codec.encode(new Message.NotFoundResponse());
        } catch (NotOwnerException | BoxNotFoundException e) {
            // We don't own this partition. If another node does, tell the client to re-route.
            return codec.encode(movedOrNotFound(message, e));
        } catch (CandyboxException e) {
            return codec.encode(new Message.ErrorResponse(e.getClass().getSimpleName(), safe(e.getMessage())));
        } catch (RuntimeException e) {
            LOG.warn("Unexpected error handling request", e);
            return codec.encode(new Message.ErrorResponse("InternalError", safe(e.getMessage())));
        }
    }

    /** What a request needs the caller to be allowed to do, or {@code null} for SASL frames. */
    private record Access(Operation operation, Resource resource) {
    }

    private static Access requiredAccess(Message message) {
        // Cluster-level requests first; everything Box-scoped derives from boxOf().
        if (message instanceof Message.CreateBoxRequest) {
            return new Access(Operation.WRITE, Resource.CLUSTER);
        }
        if (message instanceof Message.ListBoxesRequest) {
            return new Access(Operation.READ, Resource.CLUSTER);
        }
        if (message instanceof Message.DeleteBoxRequest m) {
            return new Access(Operation.ADMIN, Resource.box(m.box()));
        }
        if (message instanceof Message.GetBoxAclRequest m) {
            return new Access(Operation.READ_ACP, Resource.box(m.box()));
        }
        if (message instanceof Message.SetBoxAclRequest m) {
            return new Access(Operation.WRITE_ACP, Resource.box(m.box()));
        }
        if (message instanceof Message.HeadBoxRequest m) {
            return new Access(Operation.READ, Resource.box(m.box()));
        }
        if (message instanceof Message.BoxInfoRequest m) {
            return new Access(Operation.READ, Resource.box(m.box()));
        }
        String box = boxOf(message);
        if (box == null) {
            return null; // SASL frames are consumed by the authentication gate before this handler
        }
        Operation operation = switch (message.opcode()) {
            case GET_CANDY, RANGE_GET_CANDY, HEAD_CANDY, LIST_CANDIES, LIST_MULTIPART_UPLOADS,
                    LIST_PARTS -> Operation.READ;
            default -> Operation.WRITE;
        };
        return new Access(operation, Resource.box(box));
    }

    private Message movedOrNotFound(Message message, RuntimeException cause) {
        Integer partition = partitionOf(message);
        String box = boxOf(message);
        if (box != null && partition != null) {
            Optional<Integer> owner = node.currentOwner(BoxName.of(box), partition);
            if (owner.isPresent() && owner.get() != node.nodeId()) {
                return new Message.MovedResponse(owner.get());
            }
            return new Message.NotFoundResponse();
        }
        if (cause instanceof NotOwnerException) {
            // A Box-level request (e.g. deleteBox) that needs partitions another node still owns.
            return new Message.ErrorResponse("NotOwnerException", safe(cause.getMessage()));
        }
        return new Message.NotFoundResponse();
    }

    /** The target Box of a Box- or partition-routed request, or {@code null} for cluster-wide ones. */
    private static String boxOf(Message message) {
        if (message instanceof Message.PutCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.GetCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.RangeGetCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.HeadCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.DeleteCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.CopyCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.RenameCandyRequest m) {
            return m.box();
        } else if (message instanceof Message.DeleteRangeRequest m) {
            return m.box();
        } else if (message instanceof Message.ListCandiesRequest m) {
            return m.box();
        } else if (message instanceof Message.CreateMultipartUploadRequest m) {
            return m.box();
        } else if (message instanceof Message.UploadPartRequest m) {
            return m.box();
        } else if (message instanceof Message.CompleteMultipartUploadRequest m) {
            return m.box();
        } else if (message instanceof Message.AbortMultipartUploadRequest m) {
            return m.box();
        } else if (message instanceof Message.ListMultipartUploadsRequest m) {
            return m.box();
        } else if (message instanceof Message.ListPartsRequest m) {
            return m.box();
        } else if (message instanceof Message.UploadPartCopyRequest m) {
            return m.box();
        }
        return null;
    }

    /**
     * The partition a request targets: the key's hash partition for keyed requests, the explicit
     * partition for fanned-out ones, {@code null} for Box-level/cluster-wide requests (no single
     * owner to redirect to).
     */
    private Integer partitionOf(Message message) {
        Integer explicit = explicitPartitionOf(message);
        if (explicit != null) {
            return explicit;
        }
        String box = boxOf(message);
        String key = routingKeyOf(message);
        if (box == null || key == null) {
            return null;
        }
        try {
            return node.descriptor(BoxName.of(box)).partitionOf(key);
        } catch (BoxNotFoundException gone) {
            return null;
        }
    }

    private static Integer explicitPartitionOf(Message message) {
        if (message instanceof Message.ListCandiesRequest m) {
            return m.partition();
        } else if (message instanceof Message.DeleteRangeRequest m) {
            return m.partition();
        } else if (message instanceof Message.ListMultipartUploadsRequest m) {
            return m.partition();
        }
        return null;
    }

    /** The key whose hash partition a keyed request routes by, or {@code null}. */
    private static String routingKeyOf(Message message) {
        if (message instanceof Message.PutCandyRequest m) {
            return m.key();
        } else if (message instanceof Message.GetCandyRequest m) {
            return m.key();
        } else if (message instanceof Message.RangeGetCandyRequest m) {
            return m.key();
        } else if (message instanceof Message.HeadCandyRequest m) {
            return m.key();
        } else if (message instanceof Message.DeleteCandyRequest m) {
            return m.key();
        } else if (message instanceof Message.CopyCandyRequest m) {
            return m.dstKey();
        } else if (message instanceof Message.RenameCandyRequest m) {
            return m.dstKey();
        } else if (message instanceof Message.CreateMultipartUploadRequest m) {
            return m.key();
        } else if (message instanceof Message.UploadPartRequest m) {
            return m.key();
        } else if (message instanceof Message.CompleteMultipartUploadRequest m) {
            return m.key();
        } else if (message instanceof Message.AbortMultipartUploadRequest m) {
            return m.key();
        } else if (message instanceof Message.ListPartsRequest m) {
            return m.key();
        } else if (message instanceof Message.UploadPartCopyRequest m) {
            return m.key();
        }
        return null;
    }

    private Message dispatch(Message message, Principal principal) {
        if (message instanceof Message.CreateBoxRequest m) {
            node.createBox(BoxName.of(m.box()), m.partitionCount());
            // The creator owns the Box (private by default). An anonymous create (auth disabled)
            // seeds nothing: a Box with no ACL falls back to authenticated-full-access.
            if (!principal.isAnonymous()) {
                node.aclStore().seed(m.box(), BoxAcl.privateTo(principal));
            }
            return new Message.OkResponse();
        } else if (message instanceof Message.GetBoxAclRequest m) {
            if (!node.boxExists(BoxName.of(m.box()))) {
                return new Message.NotFoundResponse();
            }
            return node.aclStore().get(m.box())
                    .<Message>map(acl -> new Message.BoxAclResponse(acl.owner().toString(),
                            acl.grants().stream().map(Grant::toText).toList()))
                    .orElseGet(Message.NotFoundResponse::new);
        } else if (message instanceof Message.SetBoxAclRequest m) {
            if (!node.boxExists(BoxName.of(m.box()))) {
                return new Message.NotFoundResponse();
            }
            BoxAcl acl;
            try {
                acl = new BoxAcl(Principal.parse(m.owner()),
                        m.grants().stream().map(Grant::parse).toList());
            } catch (IllegalArgumentException e) {
                throw new ValidationException("Invalid ACL: " + e.getMessage());
            }
            node.aclStore().set(m.box(), acl);
            return new Message.OkResponse();
        } else if (message instanceof Message.BoxInfoRequest m) {
            BoxName box = BoxName.of(m.box());
            if (!node.boxExists(box)) {
                return new Message.NotFoundResponse();
            }
            return new Message.BoxInfoResponse(node.descriptor(box).partitionCount());
        } else if (message instanceof Message.DeleteBoxRequest m) {
            node.deleteBox(BoxName.of(m.box()), m.force());
            return new Message.OkResponse();
        } else if (message instanceof Message.PutCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            engine.putCandy(CandyKey.of(m.key()), m.data(), m.contentType(), m.userMetadata(),
                    m.idempotencyToken());
            return new Message.OkResponse();
        } else if (message instanceof Message.GetCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CandyMetadata meta = engine.getCandy(CandyKey.of(m.key()), out);
            return new Message.CandyDataResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), out.toByteArray());
        } else if (message instanceof Message.RangeGetCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BoxEngine.RangeReadResult result;
            try {
                result = engine.getCandyRange(CandyKey.of(m.key()), m.firstByte(), m.lastByte(), out);
            } catch (IllegalArgumentException e) {
                // S3's InvalidRange — surface as a typed error the gateway can map to 416.
                throw new ValidationException("InvalidRange: " + e.getMessage());
            }
            CandyMetadata meta = result.metadata();
            return new Message.CandyDataResponse(result.contentLength(), result.totalLength(),
                    meta.contentType(), meta.userMetadata(), meta.crc32c(), out.toByteArray());
        } else if (message instanceof Message.HeadCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            CandyMetadata meta = engine.headCandy(CandyKey.of(m.key()));
            return new Message.HeadCandyResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), meta.createdAtMillis());
        } else if (message instanceof Message.DeleteCandyRequest m) {
            node.engine(BoxName.of(m.box()), m.key()).deleteCandy(CandyKey.of(m.key()));
            return new Message.OkResponse();
        } else if (message instanceof Message.CopyCandyRequest m) {
            CandyMetadata meta = samePartitionEngine(m.box(), m.srcKey(), m.dstKey())
                    .copyCandy(CandyKey.of(m.srcKey()), CandyKey.of(m.dstKey()),
                            m.idempotencyToken());
            return new Message.HeadCandyResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), meta.createdAtMillis());
        } else if (message instanceof Message.RenameCandyRequest m) {
            CandyMetadata meta = samePartitionEngine(m.box(), m.srcKey(), m.dstKey())
                    .renameCandy(CandyKey.of(m.srcKey()), CandyKey.of(m.dstKey()),
                            m.idempotencyToken());
            return new Message.HeadCandyResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), meta.createdAtMillis());
        } else if (message instanceof Message.DeleteRangeRequest m) {
            BoxEngine engine = node.enginePartition(BoxName.of(m.box()), m.partition());
            if (m.prefix() != null) {
                engine.deleteRangeByPrefix(m.prefix());
            } else {
                CandyKey start = m.startKey() == null ? null : CandyKey.of(m.startKey());
                CandyKey end = m.endKey() == null ? null : CandyKey.of(m.endKey());
                engine.deleteRange(start, end);
            }
            return new Message.OkResponse();
        } else if (message instanceof Message.ListCandiesRequest m) {
            BoxEngine engine = node.enginePartition(BoxName.of(m.box()), m.partition());
            ListResult result = engine.scanCandies(toScanQuery(m));
            List<Message.ListedCandy> entries = new ArrayList<>();
            for (ListResult.ListEntry e : result.entries()) {
                entries.add(new Message.ListedCandy(e.key().value(), e.contentLength(),
                        e.createdAtMillis()));
            }
            return new Message.ListCandiesResponse(entries, result.nextStartAfter());
        } else if (message instanceof Message.ListBoxesRequest) {
            // Box names are only revealed to principals allowed to READ them.
            List<String> visible = node.listBoxes().stream()
                    .filter(box -> node.authorizer().authorize(principal, Operation.READ,
                            Resource.box(box)))
                    .toList();
            return new Message.ListBoxesResponse(visible);
        } else if (message instanceof Message.HeadBoxRequest m) {
            return node.boxExists(BoxName.of(m.box()))
                    ? new Message.OkResponse()
                    : new Message.NotFoundResponse();
        } else if (message instanceof Message.CreateMultipartUploadRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            String uploadId = engine.createMultipartUpload(CandyKey.of(m.key()), m.contentType(),
                    m.userMetadata());
            return new Message.CreateMultipartUploadResponse(uploadId);
        } else if (message instanceof Message.UploadPartRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            BoxEngine.PartUploadResult r = engine.uploadPart(m.uploadId(), m.partNumber(), m.data());
            return new Message.UploadPartResponse(r.crc32c(), r.partLength());
        } else if (message instanceof Message.CompleteMultipartUploadRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            java.util.List<BoxEngine.PartCompletion> parts = new java.util.ArrayList<>(m.parts().size());
            for (Message.CompletedPart p : m.parts()) {
                parts.add(new BoxEngine.PartCompletion(p.partNumber(), p.crc32c()));
            }
            CandyMetadata meta = engine.completeMultipartUpload(m.uploadId(), parts, m.idempotencyToken());
            return new Message.HeadCandyResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), meta.createdAtMillis());
        } else if (message instanceof Message.AbortMultipartUploadRequest m) {
            node.engine(BoxName.of(m.box()), m.key()).abortMultipartUpload(m.uploadId());
            return new Message.OkResponse();
        } else if (message instanceof Message.ListMultipartUploadsRequest m) {
            BoxEngine engine = node.enginePartition(BoxName.of(m.box()), m.partition());
            java.util.List<Message.InProgressUpload> rows = new java.util.ArrayList<>();
            String prefix = m.prefix() == null ? "" : m.prefix();
            int limit = m.maxUploads() <= 0 ? 1000 : m.maxUploads();
            String nextKey = null;
            String nextUpl = null;
            for (MultipartUploadState u : engine.listMultipartUploads()) {
                if (!prefix.isEmpty() && !u.key().startsWith(prefix)) {
                    continue;
                }
                if (m.keyMarker() != null && u.key().compareTo(m.keyMarker()) < 0) {
                    continue;
                }
                if (m.keyMarker() != null && u.key().equals(m.keyMarker())
                        && m.uploadIdMarker() != null
                        && u.uploadId().compareTo(m.uploadIdMarker()) <= 0) {
                    continue;
                }
                if (rows.size() == limit) {
                    nextKey = u.key();
                    nextUpl = u.uploadId();
                    break;
                }
                rows.add(new Message.InProgressUpload(u.uploadId(), u.key(), u.createdAtMillis()));
            }
            return new Message.ListMultipartUploadsResponse(rows, nextKey, nextUpl);
        } else if (message instanceof Message.ListPartsRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            MultipartUploadState upload = engine.multipartUpload(m.uploadId());
            if (upload == null) {
                return new Message.NotFoundResponse();
            }
            int marker = m.partNumberMarker();
            int limit = m.maxParts() <= 0 ? 1000 : m.maxParts();
            java.util.List<Message.UploadedPart> rows = new java.util.ArrayList<>();
            int next = 0;
            for (java.util.Map.Entry<Integer, Part> e : upload.parts().entrySet()) {
                int pn = e.getKey();
                if (pn <= marker) {
                    continue;
                }
                if (rows.size() == limit) {
                    next = pn - 1; // continuation cursor (exclusive in S3 ListParts)
                    break;
                }
                Part p = e.getValue();
                rows.add(new Message.UploadedPart(pn, p.partLength(), p.crc32c()));
            }
            return new Message.ListPartsResponse(rows, next);
        } else if (message instanceof Message.UploadPartCopyRequest m) {
            BoxEngine engine = samePartitionEngine(m.box(), m.srcKey(), m.key());
            try {
                BoxEngine.PartUploadResult r = engine.uploadPartCopy(m.uploadId(), m.partNumber(),
                        CandyKey.of(m.srcKey()), m.firstByte(), m.lastByte());
                return new Message.UploadPartResponse(r.crc32c(), r.partLength());
            } catch (IllegalArgumentException e) {
                throw new ValidationException("InvalidRange: " + e.getMessage());
            }
        }
        return new Message.ErrorResponse("UnsupportedOperation",
                "Not implemented in this phase: " + message.opcode());
    }

    /**
     * The engine shared by two keys of the same Box — the zero-copy copy/rename/upload-part-copy
     * path is only valid when both keys hash to the same partition (one manifest tracks the shared
     * Syrup segments); the client falls back to a byte copy otherwise.
     */
    private BoxEngine samePartitionEngine(String box, String srcKey, String dstKey) {
        BoxDescriptor descriptor = node.descriptor(BoxName.of(box));
        int srcPartition = descriptor.partitionOf(srcKey);
        int dstPartition = descriptor.partitionOf(dstKey);
        if (srcPartition != dstPartition) {
            throw new ValidationException("Cross-partition server-side copy is not supported "
                    + "(src partition " + srcPartition + ", dst partition " + dstPartition
                    + "); the client must byte-copy");
        }
        return node.enginePartition(BoxName.of(box), dstPartition);
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
