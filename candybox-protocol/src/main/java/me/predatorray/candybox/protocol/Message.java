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
package me.predatorray.candybox.protocol;

import java.util.List;
import java.util.Map;

/**
 * The typed protocol messages, mapped to/from {@link Frame}s by {@link MessageCodec}. A sealed
 * hierarchy so the codec's dispatch is exhaustive.
 *
 * <p>Phase 2 scaffold: these define the wire contract for the full v1 client API. Large-object
 * streaming (chunked PUT/GET bodies rather than the inlined {@code data} here) is TODO(phase-2);
 * the records carry inline bytes for now so small objects round-trip.
 */
public sealed interface Message {

    /** The opcode that identifies this message on the wire. */
    Opcode opcode();

    // ---- Box admin requests ----------------------------------------------------------------

    /** Creates a Box. {@code partitionCount == 0} means "use the server's configured default". */
    record CreateBoxRequest(String box, int partitionCount) implements Message {
        public Opcode opcode() {
            return Opcode.CREATE_BOX;
        }

        /** Creates with the server's default partition count. */
        public CreateBoxRequest(String box) {
            this(box, 0);
        }
    }

    record DeleteBoxRequest(String box, boolean force) implements Message {
        public Opcode opcode() {
            return Opcode.DELETE_BOX;
        }
    }

    record ListBoxesRequest() implements Message {
        public Opcode opcode() {
            return Opcode.LIST_BOXES;
        }
    }

    record HeadBoxRequest(String box) implements Message {
        public Opcode opcode() {
            return Opcode.HEAD_BOX;
        }
    }

    /** Asks any node for a Box's descriptor (currently just its partition count). */
    record BoxInfoRequest(String box) implements Message {
        public Opcode opcode() {
            return Opcode.BOX_INFO;
        }
    }

    // ---- Candy requests --------------------------------------------------------------------

    record PutCandyRequest(String box, String key, String contentType,
                           Map<String, String> userMetadata, String idempotencyToken,
                           byte[] data) implements Message {
        public Opcode opcode() {
            return Opcode.PUT_CANDY;
        }
    }

    record GetCandyRequest(String box, String key) implements Message {
        public Opcode opcode() {
            return Opcode.GET_CANDY;
        }
    }

    /**
     * Range GET. The byte window is inclusive on both ends (S3 / HTTP {@code Range: bytes=A-B}
     * semantics). {@code lastByte == -1} means "to the end of the object" and is resolved by the
     * server; {@code firstByte == -1} means "the last {@code lastByte} bytes" (suffix range) and the
     * server resolves to {@code [contentLength - lastByte, contentLength - 1]}.
     */
    record RangeGetCandyRequest(String box, String key, long firstByte, long lastByte)
            implements Message {
        public Opcode opcode() {
            return Opcode.RANGE_GET_CANDY;
        }
    }

    record HeadCandyRequest(String box, String key) implements Message {
        public Opcode opcode() {
            return Opcode.HEAD_CANDY;
        }
    }

    record DeleteCandyRequest(String box, String key) implements Message {
        public Opcode opcode() {
            return Opcode.DELETE_CANDY;
        }
    }

    record CopyCandyRequest(String box, String srcKey, String dstKey, String idempotencyToken)
            implements Message {
        public Opcode opcode() {
            return Opcode.COPY_CANDY;
        }
    }

    record RenameCandyRequest(String box, String srcKey, String dstKey, String idempotencyToken)
            implements Message {
        public Opcode opcode() {
            return Opcode.RENAME_CANDY;
        }
    }

    /**
     * Deletes a key range with a single range tombstone <em>in one partition</em>; the client fans
     * the request out to every partition of the Box. Exactly one of: a {@code prefix}, or a
     * {@code [startKey, endKey)} window (either bound nullable). {@code prefix} non-null selects the
     * prefix form.
     */
    record DeleteRangeRequest(String box, int partition, String prefix, String startKey,
                              String endKey) implements Message {
        public Opcode opcode() {
            return Opcode.DELETE_RANGE;
        }
    }

    // ---- Multipart upload requests ---------------------------------------------------------

    record CreateMultipartUploadRequest(String box, String key, String contentType,
                                        Map<String, String> userMetadata) implements Message {
        public Opcode opcode() {
            return Opcode.CREATE_MULTIPART_UPLOAD;
        }
    }

    record UploadPartRequest(String box, String key, String uploadId, int partNumber, byte[] data)
            implements Message {
        public Opcode opcode() {
            return Opcode.UPLOAD_PART;
        }
    }

    /** A {@code (partNumber, crc32c)} pair as supplied by the client in CompleteMultipartUpload. */
    record CompletedPart(int partNumber, int crc32c) {
    }

    record CompleteMultipartUploadRequest(String box, String key, String uploadId,
                                          List<CompletedPart> parts, String idempotencyToken)
            implements Message {
        public Opcode opcode() {
            return Opcode.COMPLETE_MULTIPART_UPLOAD;
        }
    }

    record AbortMultipartUploadRequest(String box, String key, String uploadId) implements Message {
        public Opcode opcode() {
            return Opcode.ABORT_MULTIPART_UPLOAD;
        }
    }

    /**
     * Lists in-flight multipart uploads in one partition of a Box, narrowed by an optional key
     * prefix; the client fans out across partitions and merges.
     */
    record ListMultipartUploadsRequest(String box, int partition, String prefix, String keyMarker,
                                       String uploadIdMarker, int maxUploads) implements Message {
        public Opcode opcode() {
            return Opcode.LIST_MULTIPART_UPLOADS;
        }
    }

    /** Lists the parts recorded for one in-flight upload. */
    record ListPartsRequest(String box, String key, String uploadId, int partNumberMarker,
                            int maxParts) implements Message {
        public Opcode opcode() {
            return Opcode.LIST_PARTS;
        }
    }

    /**
     * Zero-copy server-side copy of a byte range of a live Candy into a part slot of an in-flight
     * upload. {@code firstByte} / {@code lastByte} follow the HTTP Range conventions
     * (inclusive on both ends; {@code -1} = open-ended).
     */
    record UploadPartCopyRequest(String box, String key, String uploadId, int partNumber,
                                 String srcKey, long firstByte, long lastByte) implements Message {
        public Opcode opcode() {
            return Opcode.UPLOAD_PART_COPY;
        }
    }

    /** Lists live Candies in one partition of a Box; the client fans out and merge-sorts pages. */
    record ListCandiesRequest(String box, int partition, String prefix, String startAfter,
                              int maxKeys, String startKey, String endKey, boolean reverse)
            implements Message {
        public Opcode opcode() {
            return Opcode.LIST_CANDIES;
        }

        /** A plain forward prefix/startAfter listing of one partition. */
        public ListCandiesRequest(String box, int partition, String prefix, String startAfter,
                                  int maxKeys) {
            this(box, partition, prefix, startAfter, maxKeys, null, null, false);
        }
    }

    // ---- Responses -------------------------------------------------------------------------

    record OkResponse() implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_OK;
        }
    }

    record ErrorResponse(String errorType, String message) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_ERROR;
        }
    }

    /** Retriable backpressure signal (write-stall). */
    record BusyResponse(long retryAfterMillis) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_BUSY;
        }
    }

    record NotFoundResponse() implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_NOT_FOUND;
        }
    }

    /**
     * Candy bytes inlined in the response. {@code contentLength} is the bytes returned in {@code data}
     * (for a Range GET this is the slice length; for a full GET this is the whole object length and
     * equals {@code totalLength}). {@code totalLength} is always the whole object length so the
     * gateway can synthesize {@code Content-Range: bytes A-B/<totalLength>} on a 206 response.
     */
    record CandyDataResponse(long contentLength, long totalLength, String contentType,
                             Map<String, String> userMetadata, int crc32c, byte[] data)
            implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_CANDY_DATA;
        }

        /** Backward-compatible constructor for non-range responses (totalLength = contentLength). */
        public CandyDataResponse(long contentLength, String contentType,
                                 Map<String, String> userMetadata, int crc32c, byte[] data) {
            this(contentLength, contentLength, contentType, userMetadata, crc32c, data);
        }
    }

    record ListCandiesResponse(List<ListedCandy> entries, String nextStartAfter) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_LIST;
        }
    }

    /** One row in a {@link ListCandiesResponse}. */
    record ListedCandy(String key, long contentLength, long createdAtMillis) {
    }

    /** Candy metadata only (the {@code headCandy} response). */
    record HeadCandyResponse(long contentLength, String contentType,
                             Map<String, String> userMetadata, int crc32c, long createdAtMillis)
            implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_HEAD;
        }
    }

    /** Tells the client which node owns the requested partition, so it can re-route. */
    record MovedResponse(int ownerNodeId) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_MOVED;
        }
    }

    /** A Box's descriptor: its (creation-time-fixed) partition count. */
    record BoxInfoResponse(int partitionCount) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_BOX_INFO;
        }
    }

    record ListBoxesResponse(List<String> boxes) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_BOX_LIST;
        }
    }

    // ---- Multipart upload responses --------------------------------------------------------

    record CreateMultipartUploadResponse(String uploadId) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_CREATE_MULTIPART;
        }
    }

    record UploadPartResponse(int crc32c, long partLength) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_UPLOAD_PART;
        }
    }

    /** One row in a {@link ListMultipartUploadsResponse}. */
    record InProgressUpload(String uploadId, String key, long createdAtMillis) {
    }

    record ListMultipartUploadsResponse(List<InProgressUpload> uploads, String nextKeyMarker,
                                        String nextUploadIdMarker) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_LIST_MULTIPART_UPLOADS;
        }
    }

    /** One row in a {@link ListPartsResponse}. */
    record UploadedPart(int partNumber, long partLength, int crc32c) {
    }

    record ListPartsResponse(List<UploadedPart> parts, int nextPartNumberMarker) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_LIST_PARTS;
        }
    }
}
