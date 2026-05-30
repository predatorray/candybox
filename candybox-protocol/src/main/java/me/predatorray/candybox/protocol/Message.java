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

    record CreateBoxRequest(String box) implements Message {
        public Opcode opcode() {
            return Opcode.CREATE_BOX;
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
     * Deletes a key range with a single range tombstone. Exactly one of: a {@code prefix}, or a
     * {@code [startKey, endKey)} window (either bound nullable). {@code prefix} non-null selects the
     * prefix form.
     */
    record DeleteRangeRequest(String box, String prefix, String startKey, String endKey)
            implements Message {
        public Opcode opcode() {
            return Opcode.DELETE_RANGE;
        }
    }

    record ListCandiesRequest(String box, String prefix, String startAfter, int maxKeys,
                              String startKey, String endKey, boolean reverse) implements Message {
        public Opcode opcode() {
            return Opcode.LIST_CANDIES;
        }

        /** A plain forward prefix/startAfter listing (the classic three-arg form). */
        public ListCandiesRequest(String box, String prefix, String startAfter, int maxKeys) {
            this(box, prefix, startAfter, maxKeys, null, null, false);
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

    record CandyDataResponse(long contentLength, String contentType,
                             Map<String, String> userMetadata, int crc32c, byte[] data)
            implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_CANDY_DATA;
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

    /** Tells the client which node now owns the Box, so it can re-route (Phase 2 routing). */
    record MovedResponse(int ownerNodeId) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_MOVED;
        }
    }

    record ListBoxesResponse(List<String> boxes) implements Message {
        public Opcode opcode() {
            return Opcode.RESPONSE_BOX_LIST;
        }
    }
}
