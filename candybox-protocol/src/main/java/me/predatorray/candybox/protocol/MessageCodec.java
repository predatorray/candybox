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
        } else if (message instanceof Message.HeadCandyRequest m) {
            writeBoxKey(w, m.box(), m.key());
        } else if (message instanceof Message.DeleteCandyRequest m) {
            writeBoxKey(w, m.box(), m.key());
        } else if (message instanceof Message.ListCandiesRequest m) {
            w.writeString(m.box());
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
        } else if (message instanceof Message.ListBoxesResponse m) {
            w.writeVarInt(m.boxes().size());
            for (String box : m.boxes()) {
                w.writeString(box);
            }
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
            case CREATE_BOX -> new Message.CreateBoxRequest(r.readString());
            case DELETE_BOX -> new Message.DeleteBoxRequest(r.readString(), r.readBoolean());
            case LIST_BOXES -> new Message.ListBoxesRequest();
            case HEAD_BOX -> new Message.HeadBoxRequest(r.readString());
            case PUT_CANDY -> new Message.PutCandyRequest(r.readString(), r.readString(),
                    readNullable(r), readMetadata(r), readNullable(r), r.readBytes());
            case GET_CANDY -> new Message.GetCandyRequest(r.readString(), r.readString());
            case HEAD_CANDY -> new Message.HeadCandyRequest(r.readString(), r.readString());
            case DELETE_CANDY -> new Message.DeleteCandyRequest(r.readString(), r.readString());
            case LIST_CANDIES -> new Message.ListCandiesRequest(r.readString(), readNullable(r),
                    readNullable(r), r.readInt(), readNullable(r), readNullable(r), r.readBoolean());
            case RESPONSE_OK -> new Message.OkResponse();
            case RESPONSE_ERROR -> new Message.ErrorResponse(r.readString(), r.readString());
            case RESPONSE_BUSY -> new Message.BusyResponse(r.readVarLong());
            case RESPONSE_NOT_FOUND -> new Message.NotFoundResponse();
            case RESPONSE_CANDY_DATA -> new Message.CandyDataResponse(r.readVarLong(), readNullable(r),
                    readMetadata(r), r.readInt(), r.readBytes());
            case RESPONSE_LIST -> decodeList(r);
            case RESPONSE_HEAD -> new Message.HeadCandyResponse(r.readVarLong(), readNullable(r),
                    readMetadata(r), r.readInt(), r.readVarLong());
            case RESPONSE_MOVED -> new Message.MovedResponse(r.readInt());
            case RESPONSE_BOX_LIST -> decodeBoxList(r);
        };
    }

    private static Message decodeBoxList(BinaryReader r) {
        int count = r.readVarInt();
        List<String> boxes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            boxes.add(r.readString());
        }
        return new Message.ListBoxesResponse(boxes);
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
