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

import me.predatorray.candybox.common.namespace.NamespaceKeys;
import me.predatorray.candybox.common.namespace.NamespaceOperation;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.auth.Grant;
import me.predatorray.candybox.common.auth.ObjectAcl;
import me.predatorray.candybox.common.auth.Operation;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.auth.Resource;
import me.predatorray.candybox.common.exception.AccessDeniedException;
import me.predatorray.candybox.common.exception.AuthenticationException;
import me.predatorray.candybox.common.exception.BoxAlreadyExistsException;
import me.predatorray.candybox.common.exception.BoxNotEmptyException;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.common.exception.ValidationException;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.engine.CandyMetadata;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.lsm.manifest.RenameIntent;
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

    /** Retry hint for a request that lands in a partition's ownership handover gap. */
    private static final long UNOWNED_RETRY_AFTER_MILLIS = 200;

    /** Relay tokens are server-minted UUIDs; anything else never names a relay key. */
    private static final Pattern RELAY_TOKEN =
            Pattern.compile("[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}");

    /** The listing order for in-flight uploads, shared with the client's cross-partition merge. */
    private static final java.util.Comparator<MultipartUploadState> UPLOAD_ORDER =
            java.util.Comparator.comparing(MultipartUploadState::key, CandyKey::compareUtf8)
                    .thenComparing(MultipartUploadState::uploadId);

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
                    && !node.authorizer().authorize(principal, access.operation(), access.resource())
                    && !objectGrantPermits(message, principal, access.operation())) {
                return codec.encode(new Message.AccessDeniedResponse(
                        principal + " is not allowed " + access.operation() + " on "
                                + access.resource()));
            }
            String sourceKey = sourceKeyOf(message);
            if (sourceKey != null && !canRead(principal, boxOf(message), sourceKey)) {
                return codec.encode(new Message.AccessDeniedResponse(
                        principal + " is not allowed READ on " + boxOf(message) + "/" + sourceKey));
            }
            return codec.encode(dispatch(message, principal));
        } catch (AccessDeniedException e) {
            return codec.encode(new Message.AccessDeniedResponse(safe(e.getMessage())));
        } catch (BusyException e) {
            return codec.encode(new Message.BusyResponse(100));
        } catch (CandyNotFoundException e) {
            return codec.encode(new Message.NotFoundResponse());
        } catch (NotOwnerException | BoxNotFoundException e) {
            // We don't own this partition: re-route, retry later, or the Box is gone.
            return codec.encode(movedOrNotFound(message, e));
        } catch (CandyboxException e) {
            String errorType = e.getClass().getSimpleName();
            if (e instanceof ValidationException || e instanceof BoxAlreadyExistsException
                    || e instanceof BoxNotEmptyException || e instanceof AuthenticationException) {
                return codec.encode(new Message.ErrorResponse(errorType, safe(e.getMessage())));
            }
            return codec.encode(internalError(errorType, e));
        } catch (RuntimeException e) {
            return codec.encode(internalError("InternalError", e));
        }
    }

    /** Internal details stay in the server log; the reference lets operators find the failure. */
    private static Message.ErrorResponse internalError(String errorType, RuntimeException cause) {
        String reference = UUID.randomUUID().toString();
        LOG.warn("Error handling request; reference={}", reference, cause);
        return new Message.ErrorResponse(errorType, "Internal server error. Reference: " + reference);
    }

    /** Reads larger than this would not fit in one response frame, so they are refused unread. */
    private int maxInlineDataBytes() {
        return Message.maxInlineDataBytes(node.config().maxFrameSizeBytes());
    }

    /** Required permission, or {@code null} for routing metadata and SASL frames. */
    private record Access(Operation operation, Resource resource) {
    }

    /**
     * The S3 union rule: a READ / READ_ACP / WRITE_ACP the Box ACL denies is still allowed when the
     * <em>object's own</em> grants (locator v3) permit it. Only single-object read-side requests
     * qualify. Missing objects and malformed keys deny without revealing object existence.
     * Ownership failures follow the normal routing response so the owner can evaluate the grants;
     * redirects reveal partition ownership, never whether the object exists.
     */
    private boolean objectGrantPermits(Message message, Principal principal, Operation operation) {
        String box = boxOf(message);
        String key = switch (message.opcode()) {
            case GET_CANDY, RANGE_GET_CANDY, HEAD_CANDY -> routingKeyOf(message);
            case GET_CANDY_ACL -> ((Message.GetCandyAclRequest) message).key();
            case SET_CANDY_ACL -> ((Message.SetCandyAclRequest) message).key();
            default -> null;
        };
        return objectGrantPermits(box, key, principal, operation);
    }

    private boolean objectGrantPermits(String box, String key, Principal principal,
                                       Operation operation) {
        if (box == null || key == null) {
            return false;
        }
        try {
            ObjectAcl acl = node.engine(BoxName.of(box), key).getCandyAcl(CandyKey.of(key));
            return acl.permits(principal, operation);
        } catch (NotOwnerException | BoxNotFoundException routingFailed) {
            throw routingFailed;
        } catch (CandyboxException | IllegalArgumentException resolveFailed) {
            return false;
        }
    }

    /**
     * The source key a copy or move reads. S3 CopyObject needs READ on the source on top of WRITE
     * on the destination; otherwise a WRITE-only caller could copy an object into one it owns and
     * read that. A move is a copy plus a delete, and its response carries the source's head.
     */
    private static String sourceKeyOf(Message message) {
        if (message instanceof Message.CopyCandyRequest m) {
            return m.srcKey();
        } else if (message instanceof Message.GetCandyLocatorRequest m) {
            return m.key();
        } else if (message instanceof Message.UploadPartCopyRequest m) {
            return m.srcKey();
        } else if (message instanceof Message.RenameCandyRequest m) {
            return m.srcKey();
        } else if (message instanceof Message.PrepareRenameRequest m) {
            return m.srcKey();
        }
        return null;
    }

    private boolean canRead(Principal principal, String box, String key) {
        return node.authorizer().authorize(principal, Operation.READ, Resource.box(box))
                || objectGrantPermits(box, key, principal, Operation.READ);
    }

    /**
     * The preflight grant lookup only rejects clearly denied requests. This check must also run on
     * the ACL resolved by the actual operation, so an intervening overwrite cannot grant access to
     * a different version. Resolve Box permission before entering an engine write lock; the returned
     * callback only examines the immutable ACL passed by the engine.
     */
    private Consumer<ObjectAcl> sourceReadCheck(Principal principal, String box, String key) {
        boolean boxRead = node.authorizer().authorize(principal, Operation.READ, Resource.box(box));
        return acl -> {
            if (!boxRead && !acl.permits(principal, Operation.READ)) {
                throw new AccessDeniedException(principal + " is not allowed READ on " + box + "/" + key);
            }
        };
    }

    private static Access requiredAccess(Message message) {
        // Cluster-level requests first; everything Box-scoped derives from boxOf().
        if (message instanceof Message.CreateBoxRequest || message instanceof Message.CreateNamespaceBoxRequest) {
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
        if (message instanceof Message.GetCandyAclRequest m) {
            return new Access(Operation.READ_ACP, Resource.box(m.box()));
        }
        if (message instanceof Message.SetCandyAclRequest m) {
            return new Access(Operation.WRITE_ACP, Resource.box(m.box()));
        }
        if (message instanceof Message.HeadBoxRequest m) {
            return new Access(Operation.READ, Resource.box(m.box()));
        }
        if (message instanceof Message.BoxInfoRequest) {
            // Partition discovery precedes every fresh client's keyed request, including object-only
            // READ/READ_ACP/WRITE_ACP grants. This reveals only Box existence and partition count;
            // the eventual data/ACL request must still pass its own authorization check at the owner.
            return null;
        }
        String box = boxOf(message);
        if (message instanceof Message.NamespaceRequest m) {
            Operation permission = switch (m.operation()) {
                case GET, HEAD, LIST -> Operation.READ;
                case CLAIM_EXPIRY, CLEANUP, ENUMERATE -> Operation.ADMIN;
                default -> Operation.WRITE;
            };
            return new Access(permission, Resource.box(m.box()));
        }
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
            // No one else serves it. Only a missing Box is NOT_FOUND; otherwise it is a handover gap.
            return node.boxExists(BoxName.of(box))
                    ? new Message.UnavailableResponse(UNOWNED_RETRY_AFTER_MILLIS)
                    : new Message.NotFoundResponse();
        }
        if (cause instanceof NotOwnerException) {
            // A Box-level request (e.g. deleteBox) that needs partitions another node still owns.
            return new Message.ErrorResponse("NotOwnerException", safe(cause.getMessage()));
        }
        return new Message.NotFoundResponse();
    }

    /** The target Box of a Box- or partition-routed request, or {@code null} for cluster-wide ones. */
    private static String boxOf(Message message) {
        if (message instanceof Message.NamespaceRequest m) {
            return m.box();
        } else if (message instanceof Message.PutCandyRequest m) {
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
        } else if (message instanceof Message.GetCandyLocatorRequest m) {
            return m.box();
        } else if (message instanceof Message.PrepareRenameRequest m) {
            return m.box();
        } else if (message instanceof Message.ZeroCopyPutRequest m) {
            return m.box();
        } else if (message instanceof Message.CompleteRenameRequest m) {
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
        } else if (message instanceof Message.GetCandyAclRequest m) {
            return m.box();
        } else if (message instanceof Message.SetCandyAclRequest m) {
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
        if (message instanceof Message.NamespaceRequest m
                && m.operation() == NamespaceOperation.ENUMERATE) {
            return m.partition();
        } else if (message instanceof Message.ListCandiesRequest m) {
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
        if (message instanceof Message.NamespaceRequest m) {
            return m.address() == null ? null
                    : NamespaceKeys.lifecycle(m.address());
        } else if (message instanceof Message.PutCandyRequest m) {
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
        } else if (message instanceof Message.GetCandyLocatorRequest m) {
            return m.key();
        } else if (message instanceof Message.PrepareRenameRequest m) {
            return m.srcKey();
        } else if (message instanceof Message.ZeroCopyPutRequest m) {
            return m.dstKey();
        } else if (message instanceof Message.CompleteRenameRequest m) {
            return m.srcKey();
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
        } else if (message instanceof Message.GetCandyAclRequest m) {
            return m.key();
        } else if (message instanceof Message.SetCandyAclRequest m) {
            return m.key();
        }
        return null;
    }

    private Message dispatch(Message message, Principal principal) {
        if (message instanceof Message.CreateNamespaceBoxRequest m) {
            node.createBox(BoxName.of(m.box()), m.partitionCount(), true);
            if (!principal.isAnonymous()) {
                node.aclStore().seed(m.box(), BoxAcl.privateTo(principal));
            }
            return new Message.OkResponse();
        }
        String target = boxOf(message);
        if (target != null && node.descriptor(BoxName.of(target)).namespaceManaged()) {
            if (message instanceof Message.NamespaceRequest m) {
                BoxEngine engine = m.operation() == NamespaceOperation.ENUMERATE
                        ? node.enginePartition(BoxName.of(target), m.partition())
                        : node.engine(BoxName.of(target), routingKeyOf(message));
                if (m.data().length > maxInlineDataBytes()) {
                    throw new ValidationException("Namespace payload exceeds inline frame budget");
                }
                if (m.pageSize() < 1 || m.pageSize() > 128) {
                    throw new ValidationException("Namespace page size must be 1..128");
                }
                int pageSize = m.pageSize();
                if (m.operation() == NamespaceOperation.LIST || m.operation() == NamespaceOperation.ENUMERATE) {
                    long rowBudget = (long) node.config().sizeLimits().maxCandyKeyBytes() + 16;
                    long rows = maxInlineDataBytes() / rowBudget;
                    if (rows < 1) { throw new ValidationException("Namespace page cannot fit frame budget"); }
                    pageSize = (int) Math.min(pageSize, rows);
                }
                var result = engine.namespace(m.operation(), m.address(), m.key(), m.data(),
                        m.idleTtlMillis(), m.expectedVersion(), pageSize, m.cursor(), maxInlineDataBytes());
                return new Message.NamespaceResponse(result.outcome(), result.state(), result.data(),
                        result.keys(), result.cursor());
            }
            if (!(message instanceof Message.GetBoxAclRequest)
                    && !(message instanceof Message.SetBoxAclRequest)) {
                throw new ValidationException("Managed Box requires namespace operations");
            }
        } else if (message instanceof Message.NamespaceRequest) {
            throw new ValidationException("Namespace operation requires a managed Box");
        }

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
            engine.putCandy(CandyKey.of(m.key()),
                    new java.io.ByteArrayInputStream(m.data() == null ? new byte[0] : m.data()),
                    m.contentType(), m.userMetadata(), m.idempotencyToken(),
                    effectiveAcl(principal, m.owner(), m.grants()));
            return new Message.OkResponse();
        } else if (message instanceof Message.GetCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CandyMetadata meta = engine.getCandy(CandyKey.of(m.key()), out, maxInlineDataBytes(),
                    sourceReadCheck(principal, m.box(), m.key()));
            return new Message.CandyDataResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), out.toByteArray());
        } else if (message instanceof Message.RangeGetCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BoxEngine.RangeReadResult result;
            try {
                result = engine.getCandyRange(CandyKey.of(m.key()), m.firstByte(), m.lastByte(), out,
                        maxInlineDataBytes(), m.minSourceBytesExclusive(),
                        sourceReadCheck(principal, m.box(), m.key()));
            } catch (IllegalArgumentException e) {
                // S3's InvalidRange — surface as a typed error the gateway can map to 416.
                throw new ValidationException("InvalidRange: " + e.getMessage());
            }
            CandyMetadata meta = result.metadata();
            return new Message.CandyDataResponse(result.contentLength(), result.totalLength(),
                    meta.contentType(), meta.userMetadata(), meta.crc32c(), out.toByteArray());
        } else if (message instanceof Message.HeadCandyRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            CandyMetadata meta = engine.headCandy(CandyKey.of(m.key()),
                    sourceReadCheck(principal, m.box(), m.key()));
            return new Message.HeadCandyResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), meta.createdAtMillis());
        } else if (message instanceof Message.DeleteCandyRequest m) {
            node.engine(BoxName.of(m.box()), m.key()).deleteCandy(CandyKey.of(m.key()));
            return new Message.OkResponse();
        } else if (message instanceof Message.CopyCandyRequest m) {
            CandyMetadata meta = samePartitionEngine(m.box(), m.srcKey(), m.dstKey())
                    .copyCandy(CandyKey.of(m.srcKey()), CandyKey.of(m.dstKey()),
                            m.idempotencyToken(), effectiveAcl(principal, m.owner(), m.grants()),
                            sourceReadCheck(principal, m.box(), m.srcKey()));
            return new Message.HeadCandyResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), meta.createdAtMillis());
        } else if (message instanceof Message.RenameCandyRequest m) {
            CandyMetadata meta = samePartitionEngine(m.box(), m.srcKey(), m.dstKey())
                    .renameCandy(CandyKey.of(m.srcKey()), CandyKey.of(m.dstKey()),
                            m.idempotencyToken(), sourceReadCheck(principal, m.box(), m.srcKey()));
            return new Message.HeadCandyResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), meta.createdAtMillis());
        } else if (message instanceof Message.GetCandyLocatorRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.key());
            CandyLocator locator = engine.resolveLocator(CandyKey.of(m.key()));
            sourceReadCheck(principal, m.box(), m.key()).accept(locator.acl());
            return relayLocator(engine, m.box(), m.key(), principal, null, null, locator);
        } else if (message instanceof Message.PrepareRenameRequest m) {
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.srcKey());
            // The same source-engine monitor protects maintenance retirement and preparation.
            synchronized (engine) {
                CandyLocator locator = engine.resolveLocator(CandyKey.of(m.srcKey()));
                sourceReadCheck(principal, m.box(), m.srcKey()).accept(locator.acl());
                var prepared = relayLocator(engine, m.box(), m.srcKey(), principal, m.renameToken(),
                        m.dstKey(), locator);
                try {
                    node.prepareRenameMarker(m.box(), m.renameToken(), m.srcKey(),
                            node.descriptor(BoxName.of(m.box())).partitionOf(m.srcKey()), locator.hlc(), m.dstKey());
                    engine.recordRenameIntent(new RenameIntent(m.renameToken(), m.srcKey(), locator.hlc(),
                            m.dstKey(), m.dstPartition(), node.currentTimeMillis()));
                    return prepared;
                } catch (RuntimeException e) {
                    node.deleteLocatorRelay(m.box(), node.descriptor(BoxName.of(m.box())).partitionOf(m.srcKey()),
                            prepared.relayToken());
                    throw e;
                }
            }
        } else if (message instanceof Message.ZeroCopyPutRequest m) {
            BoxName box = BoxName.of(m.box());
            BoxEngine engine = node.engine(box, m.dstKey());
            // The parts and a rename's ACL come from the source owner's relay, never from the client.
            LocatorRelay relay = consumableRelay(m, principal, engine);
            CandyLocator source = relay.locator();
            // A rename keeps the object's identity, so the source's owner/grants travel with it; a
            // copy belongs to the requester (effectiveAcl applies the super-user owner rule).
            ObjectAcl acl = relay.isRename()
                    ? source.acl()
                    : effectiveAcl(principal, m.owner(), m.grants());
            CandyMetadata meta = relay.consumer().committed();
            if (meta == null) {
                // Share admission with fenced-claim retirement, including its receipt-absence check.
                synchronized (engine) {
                    meta = relay.isRename()
                            ? engine.zeroCopyRename(CandyKey.of(m.dstKey()), source, relay.renameToken(),
                                    relay.consumer().incarnation(), m.relayToken(), m.srcPartition(), relay.srcKey(),
                                    () -> node.requirePendingRename(m.box(), m.srcPartition(), m.relayToken()))
                            : engine.zeroCopyPut(CandyKey.of(m.dstKey()), source.parts(),
                                    source.contentType(), source.userMetadata(), 0L, acl, m.idempotencyToken());
                }
                relay = node.recordCommittedLocatorRelay(m.box(), m.srcPartition(), m.relayToken(),
                        relay, meta);
            }
            // Publish this partition's refs before the source delete can run, so the Box-global GC
            // already sees the new cross-partition reference (no premature reclaim of the shared Syrup).
            node.publishPartitionRefs(box, node.descriptor(box).partitionOf(m.dstKey()));
            if (relay.isRename()) {
                node.publishRenameMarker(m.box(), relay.renameToken(), relay.srcKey(),
                        m.srcPartition(), source.hlc(), m.dstKey(), relay.consumer().incarnation());
            }
            // Only now: committed retries finish publication without ever appending another locator.
            // Rename claims remain the durable transfer decision until the source acknowledges.
            if (!relay.isRename()) {
                node.completeLocatorRelay(m.box(), m.srcPartition(), m.relayToken(), relay);
            }
            return new Message.HeadCandyResponse(meta.contentLength(), meta.contentType(),
                    meta.userMetadata(), meta.crc32c(), meta.createdAtMillis());
        } else if (message instanceof Message.CompleteRenameRequest m) {
            // The source partition's owner finalizes the rename (also reachable via the maintenance
            // sweep / after handover): LWW-conditioned delete of the source, then clear intent + marker.
            BoxEngine engine = node.engine(BoxName.of(m.box()), m.srcKey());
            node.finalizeRenameIntent(engine, m.box(), m.renameToken(), m.srcKey());
            return new Message.OkResponse();
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
            return new Message.ListCandiesResponse(entries, result.nextStartAfter(),
                    result.commonPrefixes());
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
            CandyMetadata meta = engine.completeMultipartUpload(m.uploadId(), parts,
                    m.idempotencyToken(), effectiveAcl(principal, m.owner(), m.grants()));
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
            String keyMarker = m.keyMarker();
            String uploadIdMarker = m.uploadIdMarker();
            // Pages must hold this partition's smallest (key, uploadId) rows so the client's
            // cross-partition merge cannot skip any; the manifest map is in creation order.
            java.util.List<MultipartUploadState> matching = new java.util.ArrayList<>();
            for (MultipartUploadState u : engine.listMultipartUploads()) {
                if (u.key().startsWith(prefix) && afterUploadMarker(u, keyMarker, uploadIdMarker)) {
                    matching.add(u);
                }
            }
            matching.sort(UPLOAD_ORDER);
            for (MultipartUploadState u : matching.subList(0, Math.min(limit, matching.size()))) {
                rows.add(new Message.InProgressUpload(u.uploadId(), u.key(), u.createdAtMillis()));
            }
            String nextKey = null;
            String nextUpl = null;
            if (matching.size() > limit) {
                Message.InProgressUpload last = rows.get(rows.size() - 1);
                nextKey = last.key();
                nextUpl = last.uploadId();
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
        } else if (message instanceof Message.GetCandyAclRequest m) {
            ObjectAcl acl = node.engine(BoxName.of(m.box()), m.key())
                    .getCandyAcl(CandyKey.of(m.key()));
            return new Message.CandyAclResponse(acl.owner(),
                    acl.grants().stream().map(Grant::toText).toList());
        } else if (message instanceof Message.SetCandyAclRequest m) {
            ObjectAcl acl;
            try {
                acl = new ObjectAcl(
                        m.owner() == null ? null : Principal.parse(m.owner()).toString(),
                        m.grants().stream().map(Grant::parse).toList());
            } catch (IllegalArgumentException e) {
                throw new ValidationException("Invalid object ACL: " + e.getMessage());
            }
            node.engine(BoxName.of(m.box()), m.key()).setCandyAcl(CandyKey.of(m.key()), acl);
            return new Message.OkResponse();
        } else if (message instanceof Message.UploadPartCopyRequest m) {
            BoxEngine engine = samePartitionEngine(m.box(), m.srcKey(), m.key());
            try {
                BoxEngine.PartUploadResult r = engine.uploadPartCopy(m.uploadId(), m.partNumber(),
                        CandyKey.of(m.srcKey()), m.firstByte(), m.lastByte(), m.minSourceBytesExclusive(),
                        sourceReadCheck(principal, m.box(), m.srcKey()));
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

    /**
     * Stashes a resolved source locator in coordination and answers with its relay handle. The
     * source is re-resolved once the relay exists, so the relay was written while the source still
     * referenced its Syrups (Box-global GC pins relayed Syrups from then on).
     */
    private Message.CandyLocatorResponse relayLocator(BoxEngine engine, String box, String srcKey,
                                                      Principal principal, String renameToken,
                                                      String dstKey, CandyLocator locator) {
        int srcPartition = node.descriptor(BoxName.of(box)).partitionOf(srcKey);
        String token = node.writeLocatorRelay(box, srcPartition, new LocatorRelay(
                principal.toString(), srcKey, renameToken, dstKey, node.currentTimeMillis(), locator));
        CandyLocator current;
        try {
            current = engine.resolveLocator(CandyKey.of(srcKey));
        } catch (RuntimeException e) {
            node.deleteLocatorRelay(box, srcPartition, token);
            throw e;
        }
        if (!current.hlc().equals(locator.hlc())) {
            node.deleteLocatorRelay(box, srcPartition, token);
            throw new BusyException("Source " + srcKey + " changed while its locator was relayed");
        }
        return new Message.CandyLocatorResponse(token, locator.hlc());
    }

    /**
     * Claims the relay a zero-copy put names, provided the same principal created it and, for a rename,
     * it was prepared for this destination key. Anything else, including an expired pending relay, is
     * indistinguishable from a missing one.
     */
    private LocatorRelay consumableRelay(Message.ZeroCopyPutRequest m, Principal principal,
                                          BoxEngine engine) {
        Optional<LocatorRelay> relay = m.relayToken() != null
                && RELAY_TOKEN.matcher(m.relayToken()).matches()
                ? node.claimLocatorRelay(m.box(), m.srcPartition(), m.relayToken(), m.dstKey(), engine,
                        r -> r.requester().equals(principal.toString())
                                && (!r.isRename() || r.dstKey().equals(m.dstKey())))
                : Optional.empty();
        return relay.orElseThrow(
                () -> new ValidationException("Unknown or expired locator relay"));
    }

    /**
     * The owner/grants a write stamps into the locator. The owner is the connection's principal
     * (null when anonymous); a request-supplied owner is honored only from super-principals — the
     * S3 gateway writing on behalf of its authenticated end user. Request grants (the S3 canned-ACL
     * path) are accepted from any writer.
     */
    private ObjectAcl effectiveAcl(Principal principal, String requestedOwner,
                                   List<String> grantTexts) {
        List<Grant> grants;
        try {
            grants = grantTexts == null ? List.of()
                    : grantTexts.stream().map(Grant::parse).toList();
            String owner = requestedOwner != null && node.authorizer().isSuperUser(principal)
                    ? Principal.parse(requestedOwner).toString()
                    : (principal.isAnonymous() ? null : principal.toString());
            return new ObjectAcl(owner, grants);
        } catch (IllegalArgumentException e) {
            throw new ValidationException("Invalid object ACL: " + e.getMessage());
        }
    }

    private static ScanQuery toScanQuery(Message.ListCandiesRequest m) {
        CandyKey start = m.startKey() == null ? null : CandyKey.of(m.startKey());
        CandyKey end = m.endKey() == null ? null : CandyKey.of(m.endKey());
        CandyKey cursor = m.startAfter() == null ? null : CandyKey.of(m.startAfter());
        ScanDirection direction = m.reverse() ? ScanDirection.REVERSE : ScanDirection.FORWARD;
        return new ScanQuery(m.prefix(), start, end, cursor, direction, m.maxKeys(),
                m.delimiter());
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    /**
     * S3 marker semantics: with only {@code keyMarker}, keys strictly after it; with both, also
     * uploads of {@code keyMarker} whose uploadId sorts after {@code uploadIdMarker}.
     */
    private static boolean afterUploadMarker(MultipartUploadState u, String keyMarker,
                                             String uploadIdMarker) {
        if (keyMarker == null) {
            return true;
        }
        int c = CandyKey.compareUtf8(u.key(), keyMarker);
        return c > 0 || (c == 0 && uploadIdMarker != null
                && u.uploadId().compareTo(uploadIdMarker) > 0);
    }
}
