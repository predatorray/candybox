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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;

/**
 * Serializes {@link Message}s to and from {@link Frame} payloads. Each message body is a versioned
 * binary record; the opcode in the frame header selects the decoder.
 */
public final class MessageCodec {

    private static final byte BODY_VERSION = 1;

    public Frame encode(Message message) {
        BinaryWriter w = new BinaryWriter(64);
        w.writeByte(BODY_VERSION);
        if (message instanceof Message.CreateBoxRequest m) {
            w.writeString(m.box());
            w.writeVarInt(m.partitionCount());
        } else if (message instanceof Message.BoxInfoRequest m) {
            w.writeString(m.box());
        } else if (message instanceof Message.DeleteBoxRequest m) {
            w.writeString(m.box());
            w.writeBoolean(m.force());
        } else if (message instanceof Message.ListBoxesRequest) {
            // no body
        } else if (message instanceof Message.HeadBoxRequest m) {
            w.writeString(m.box());
        } else if (message instanceof Message.PutCandyRequest m) {
            w.writeString(m.box());
            w.writeString(m.key());
            writeNullable(w, m.contentType());
            writeMetadata(w, m.userMetadata());
            writeNullable(w, m.idempotencyToken());
            w.writeBytes(m.data() == null ? new byte[0] : m.data());
        } else if (message instanceof Message.GetCandyRequest m) {
            writeBoxKey(w, m.box(), m.key());
        } else if (message instanceof Message.RangeGetCandyRequest m) {
            writeBoxKey(w, m.box(), m.key());
            w.writeLong(m.firstByte());
            w.writeLong(m.lastByte());
        } else if (message instanceof Message.HeadCandyRequest m) {
            writeBoxKey(w, m.box(), m.key());
        } else if (message instanceof Message.DeleteCandyRequest m) {
            writeBoxKey(w, m.box(), m.key());
        } else if (message instanceof Message.CopyCandyRequest m) {
            w.writeString(m.box());
            w.writeString(m.srcKey());
            w.writeString(m.dstKey());
            writeNullable(w, m.idempotencyToken());
        } else if (message instanceof Message.RenameCandyRequest m) {
            w.writeString(m.box());
            w.writeString(m.srcKey());
            w.writeString(m.dstKey());
            writeNullable(w, m.idempotencyToken());
        } else if (message instanceof Message.DeleteRangeRequest m) {
            w.writeString(m.box());
            w.writeVarInt(m.partition());
            writeNullable(w, m.prefix());
            writeNullable(w, m.startKey());
            writeNullable(w, m.endKey());
        } else if (message instanceof Message.CreateMultipartUploadRequest m) {
            w.writeString(m.box());
            w.writeString(m.key());
            writeNullable(w, m.contentType());
            writeMetadata(w, m.userMetadata());
        } else if (message instanceof Message.UploadPartRequest m) {
            w.writeString(m.box());
            w.writeString(m.key());
            w.writeString(m.uploadId());
            w.writeVarInt(m.partNumber());
            w.writeBytes(m.data() == null ? new byte[0] : m.data());
        } else if (message instanceof Message.CompleteMultipartUploadRequest m) {
            w.writeString(m.box());
            w.writeString(m.key());
            w.writeString(m.uploadId());
            w.writeVarInt(m.parts().size());
            for (Message.CompletedPart p : m.parts()) {
                w.writeVarInt(p.partNumber());
                w.writeInt(p.crc32c());
            }
            writeNullable(w, m.idempotencyToken());
        } else if (message instanceof Message.AbortMultipartUploadRequest m) {
            w.writeString(m.box());
            w.writeString(m.key());
            w.writeString(m.uploadId());
        } else if (message instanceof Message.ListMultipartUploadsRequest m) {
            w.writeString(m.box());
            w.writeVarInt(m.partition());
            writeNullable(w, m.prefix());
            writeNullable(w, m.keyMarker());
            writeNullable(w, m.uploadIdMarker());
            w.writeVarInt(m.maxUploads());
        } else if (message instanceof Message.ListPartsRequest m) {
            w.writeString(m.box());
            w.writeString(m.key());
            w.writeString(m.uploadId());
            w.writeVarInt(m.partNumberMarker());
            w.writeVarInt(m.maxParts());
        } else if (message instanceof Message.UploadPartCopyRequest m) {
            w.writeString(m.box());
            w.writeString(m.key());
            w.writeString(m.uploadId());
            w.writeVarInt(m.partNumber());
            w.writeString(m.srcKey());
            w.writeLong(m.firstByte());
            w.writeLong(m.lastByte());
        } else if (message instanceof Message.CreateMultipartUploadResponse m) {
            w.writeString(m.uploadId());
        } else if (message instanceof Message.UploadPartResponse m) {
            w.writeInt(m.crc32c());
            w.writeVarLong(m.partLength());
        } else if (message instanceof Message.ListMultipartUploadsResponse m) {
            w.writeVarInt(m.uploads().size());
            for (Message.InProgressUpload u : m.uploads()) {
                w.writeString(u.uploadId());
                w.writeString(u.key());
                w.writeVarLong(Math.max(0, u.createdAtMillis()));
            }
            writeNullable(w, m.nextKeyMarker());
            writeNullable(w, m.nextUploadIdMarker());
        } else if (message instanceof Message.ListPartsResponse m) {
            w.writeVarInt(m.parts().size());
            for (Message.UploadedPart p : m.parts()) {
                w.writeVarInt(p.partNumber());
                w.writeVarLong(p.partLength());
                w.writeInt(p.crc32c());
            }
            w.writeVarInt(m.nextPartNumberMarker());
        } else if (message instanceof Message.ListCandiesRequest m) {
            w.writeString(m.box());
            w.writeVarInt(m.partition());
            writeNullable(w, m.prefix());
            writeNullable(w, m.startAfter());
            w.writeInt(m.maxKeys());
            writeNullable(w, m.startKey());
            writeNullable(w, m.endKey());
            w.writeBoolean(m.reverse());
        } else if (message instanceof Message.OkResponse) {
            // no body
        } else if (message instanceof Message.ErrorResponse m) {
            w.writeString(m.errorType());
            w.writeString(m.message());
        } else if (message instanceof Message.BusyResponse m) {
            w.writeVarLong(Math.max(0, m.retryAfterMillis()));
        } else if (message instanceof Message.NotFoundResponse) {
            // no body
        } else if (message instanceof Message.CandyDataResponse m) {
            w.writeVarLong(m.contentLength());
            w.writeVarLong(m.totalLength());
            writeNullable(w, m.contentType());
            writeMetadata(w, m.userMetadata());
            w.writeInt(m.crc32c());
            w.writeBytes(m.data() == null ? new byte[0] : m.data());
        } else if (message instanceof Message.ListCandiesResponse m) {
            w.writeVarInt(m.entries().size());
            for (Message.ListedCandy e : m.entries()) {
                w.writeString(e.key());
                w.writeVarLong(e.contentLength());
                w.writeVarLong(Math.max(0, e.createdAtMillis()));
            }
            writeNullable(w, m.nextStartAfter());
        } else if (message instanceof Message.HeadCandyResponse m) {
            w.writeVarLong(m.contentLength());
            writeNullable(w, m.contentType());
            writeMetadata(w, m.userMetadata());
            w.writeInt(m.crc32c());
            w.writeVarLong(Math.max(0, m.createdAtMillis()));
        } else if (message instanceof Message.MovedResponse m) {
            w.writeInt(m.ownerNodeId());
        } else if (message instanceof Message.BoxInfoResponse m) {
            w.writeVarInt(m.partitionCount());
        } else if (message instanceof Message.ListBoxesResponse m) {
            w.writeVarInt(m.boxes().size());
            for (String box : m.boxes()) {
                w.writeString(box);
            }
        } else if (message instanceof Message.SaslHandshakeRequest m) {
            w.writeString(m.mechanism());
        } else if (message instanceof Message.SaslHandshakeResponse m) {
            w.writeBoolean(m.ok());
            w.writeVarInt(m.enabledMechanisms().size());
            for (String mechanism : m.enabledMechanisms()) {
                w.writeString(mechanism);
            }
        } else if (message instanceof Message.SaslAuthenticateRequest m) {
            w.writeBytes(m.token() == null ? new byte[0] : m.token());
        } else if (message instanceof Message.SaslAuthenticateResponse m) {
            w.writeBoolean(m.complete());
            w.writeBytes(m.challenge() == null ? new byte[0] : m.challenge());
        } else if (message instanceof Message.AuthFailedResponse m) {
            w.writeString(m.message());
        } else {
            throw new ProtocolException("Unknown message type: " + message.getClass());
        }
        return new Frame(message.opcode(), w.toByteArray());
    }

    public Message decode(Frame frame) {
        BinaryReader r = new BinaryReader(frame.payload());
        int version = r.readByte();
        if (version != BODY_VERSION) {
            throw new ProtocolException("Unsupported message body version: " + version);
        }
        return switch (frame.opcode()) {
            case CREATE_BOX -> new Message.CreateBoxRequest(r.readString(), r.readVarInt());
            case BOX_INFO -> new Message.BoxInfoRequest(r.readString());
            case DELETE_BOX -> new Message.DeleteBoxRequest(r.readString(), r.readBoolean());
            case LIST_BOXES -> new Message.ListBoxesRequest();
            case HEAD_BOX -> new Message.HeadBoxRequest(r.readString());
            case PUT_CANDY -> new Message.PutCandyRequest(r.readString(), r.readString(),
                    readNullable(r), readMetadata(r), readNullable(r), r.readBytes());
            case GET_CANDY -> new Message.GetCandyRequest(r.readString(), r.readString());
            case RANGE_GET_CANDY -> new Message.RangeGetCandyRequest(r.readString(), r.readString(),
                    r.readLong(), r.readLong());
            case HEAD_CANDY -> new Message.HeadCandyRequest(r.readString(), r.readString());
            case DELETE_CANDY -> new Message.DeleteCandyRequest(r.readString(), r.readString());
            case COPY_CANDY -> new Message.CopyCandyRequest(r.readString(), r.readString(),
                    r.readString(), readNullable(r));
            case RENAME_CANDY -> new Message.RenameCandyRequest(r.readString(), r.readString(),
                    r.readString(), readNullable(r));
            case DELETE_RANGE -> new Message.DeleteRangeRequest(r.readString(), r.readVarInt(),
                    readNullable(r), readNullable(r), readNullable(r));
            case LIST_CANDIES -> new Message.ListCandiesRequest(r.readString(), r.readVarInt(),
                    readNullable(r), readNullable(r), r.readInt(), readNullable(r), readNullable(r),
                    r.readBoolean());
            case CREATE_MULTIPART_UPLOAD -> new Message.CreateMultipartUploadRequest(r.readString(),
                    r.readString(), readNullable(r), readMetadata(r));
            case UPLOAD_PART -> new Message.UploadPartRequest(r.readString(), r.readString(),
                    r.readString(), r.readVarInt(), r.readBytes());
            case COMPLETE_MULTIPART_UPLOAD -> decodeCompleteMultipart(r);
            case ABORT_MULTIPART_UPLOAD -> new Message.AbortMultipartUploadRequest(r.readString(),
                    r.readString(), r.readString());
            case LIST_MULTIPART_UPLOADS -> new Message.ListMultipartUploadsRequest(r.readString(),
                    r.readVarInt(), readNullable(r), readNullable(r), readNullable(r), r.readVarInt());
            case LIST_PARTS -> new Message.ListPartsRequest(r.readString(), r.readString(),
                    r.readString(), r.readVarInt(), r.readVarInt());
            case UPLOAD_PART_COPY -> new Message.UploadPartCopyRequest(r.readString(), r.readString(),
                    r.readString(), r.readVarInt(), r.readString(), r.readLong(), r.readLong());
            case RESPONSE_CREATE_MULTIPART -> new Message.CreateMultipartUploadResponse(r.readString());
            case RESPONSE_UPLOAD_PART -> new Message.UploadPartResponse(r.readInt(), r.readVarLong());
            case RESPONSE_LIST_MULTIPART_UPLOADS -> decodeListMultipart(r);
            case RESPONSE_LIST_PARTS -> decodeListParts(r);
            case RESPONSE_OK -> new Message.OkResponse();
            case RESPONSE_ERROR -> new Message.ErrorResponse(r.readString(), r.readString());
            case RESPONSE_BUSY -> new Message.BusyResponse(r.readVarLong());
            case RESPONSE_NOT_FOUND -> new Message.NotFoundResponse();
            case RESPONSE_CANDY_DATA -> new Message.CandyDataResponse(r.readVarLong(), r.readVarLong(),
                    readNullable(r), readMetadata(r), r.readInt(), r.readBytes());
            case RESPONSE_LIST -> decodeList(r);
            case RESPONSE_HEAD -> new Message.HeadCandyResponse(r.readVarLong(), readNullable(r),
                    readMetadata(r), r.readInt(), r.readVarLong());
            case RESPONSE_MOVED -> new Message.MovedResponse(r.readInt());
            case RESPONSE_BOX_INFO -> new Message.BoxInfoResponse(r.readVarInt());
            case RESPONSE_BOX_LIST -> decodeBoxList(r);
            case SASL_HANDSHAKE -> new Message.SaslHandshakeRequest(r.readString());
            case RESPONSE_SASL_HANDSHAKE -> decodeSaslHandshakeResponse(r);
            case SASL_AUTHENTICATE -> new Message.SaslAuthenticateRequest(r.readBytes());
            case RESPONSE_SASL_AUTHENTICATE ->
                    new Message.SaslAuthenticateResponse(r.readBoolean(), r.readBytes());
            case RESPONSE_AUTH_FAILED -> new Message.AuthFailedResponse(r.readString());
        };
    }

    private static Message decodeSaslHandshakeResponse(BinaryReader r) {
        boolean ok = r.readBoolean();
        int count = r.readVarInt();
        List<String> mechanisms = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            mechanisms.add(r.readString());
        }
        return new Message.SaslHandshakeResponse(ok, mechanisms);
    }

    private static Message decodeBoxList(BinaryReader r) {
        int count = r.readVarInt();
        List<String> boxes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            boxes.add(r.readString());
        }
        return new Message.ListBoxesResponse(boxes);
    }

    private static Message decodeCompleteMultipart(BinaryReader r) {
        String box = r.readString();
        String key = r.readString();
        String uploadId = r.readString();
        int count = r.readVarInt();
        List<Message.CompletedPart> parts = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            parts.add(new Message.CompletedPart(r.readVarInt(), r.readInt()));
        }
        String idempotency = readNullable(r);
        return new Message.CompleteMultipartUploadRequest(box, key, uploadId, parts, idempotency);
    }

    private static Message decodeListMultipart(BinaryReader r) {
        int count = r.readVarInt();
        List<Message.InProgressUpload> uploads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            uploads.add(new Message.InProgressUpload(r.readString(), r.readString(), r.readVarLong()));
        }
        String nextKey = readNullable(r);
        String nextUploadId = readNullable(r);
        return new Message.ListMultipartUploadsResponse(uploads, nextKey, nextUploadId);
    }

    private static Message decodeListParts(BinaryReader r) {
        int count = r.readVarInt();
        List<Message.UploadedPart> parts = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            parts.add(new Message.UploadedPart(r.readVarInt(), r.readVarLong(), r.readInt()));
        }
        int nextMarker = r.readVarInt();
        return new Message.ListPartsResponse(parts, nextMarker);
    }

    private static Message decodeList(BinaryReader r) {
        int count = r.readVarInt();
        List<Message.ListedCandy> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(new Message.ListedCandy(r.readString(), r.readVarLong(), r.readVarLong()));
        }
        return new Message.ListCandiesResponse(entries, readNullable(r));
    }

    private static void writeBoxKey(BinaryWriter w, String box, String key) {
        w.writeString(box);
        w.writeString(key);
    }

    private static void writeNullable(BinaryWriter w, String s) {
        if (s == null) {
            w.writeBoolean(false);
        } else {
            w.writeBoolean(true);
            w.writeString(s);
        }
    }

    private static String readNullable(BinaryReader r) {
        return r.readBoolean() ? r.readString() : null;
    }

    private static void writeMetadata(BinaryWriter w, Map<String, String> metadata) {
        Map<String, String> md = metadata == null ? Map.of() : metadata;
        w.writeVarInt(md.size());
        for (Map.Entry<String, String> e : md.entrySet()) {
            w.writeString(e.getKey());
            w.writeString(e.getValue());
        }
    }

    private static Map<String, String> readMetadata(BinaryReader r) {
        int count = r.readVarInt();
        Map<String, String> md = new LinkedHashMap<>(Math.max(4, count * 2));
        for (int i = 0; i < count; i++) {
            md.put(r.readString(), r.readString());
        }
        return md;
    }
}
