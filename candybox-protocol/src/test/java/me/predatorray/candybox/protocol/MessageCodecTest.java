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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MessageCodecTest {

    private final MessageCodec codec = new MessageCodec();

    private Message roundTrip(Message message) {
        return codec.decode(codec.encode(message));
    }

    @Test
    void putCandyRequestRoundTrips() {
        Message.PutCandyRequest req = new Message.PutCandyRequest("my-box", "k/1", "text/plain",
                Map.of("a", "b"), "idem-1", "payload".getBytes());
        Message decoded = roundTrip(req);
        assertThat(decoded).isInstanceOf(Message.PutCandyRequest.class);
        Message.PutCandyRequest out = (Message.PutCandyRequest) decoded;
        assertThat(out.box()).isEqualTo("my-box");
        assertThat(out.key()).isEqualTo("k/1");
        assertThat(out.contentType()).isEqualTo("text/plain");
        assertThat(out.userMetadata()).containsEntry("a", "b");
        assertThat(out.idempotencyToken()).isEqualTo("idem-1");
        assertThat(new String(out.data())).isEqualTo("payload");
    }

    @Test
    void nullableFieldsRoundTrip() {
        Message.PutCandyRequest req = new Message.PutCandyRequest("box-name", "k", null,
                Map.of(), null, new byte[0]);
        Message.PutCandyRequest out = (Message.PutCandyRequest) roundTrip(req);
        assertThat(out.contentType()).isNull();
        assertThat(out.idempotencyToken()).isNull();
        assertThat(out.userMetadata()).isEmpty();
    }

    @Test
    void copyRenameAndDeleteRangeRequestsRoundTrip() {
        Message.CopyCandyRequest copy = (Message.CopyCandyRequest) roundTrip(
                new Message.CopyCandyRequest("box", "src", "dst", "idem"));
        assertThat(copy.srcKey()).isEqualTo("src");
        assertThat(copy.dstKey()).isEqualTo("dst");
        assertThat(copy.idempotencyToken()).isEqualTo("idem");

        Message.RenameCandyRequest rename = (Message.RenameCandyRequest) roundTrip(
                new Message.RenameCandyRequest("box", "a", "b", null));
        assertThat(rename.dstKey()).isEqualTo("b");
        assertThat(rename.idempotencyToken()).isNull();

        Message.DeleteRangeRequest byPrefix = (Message.DeleteRangeRequest) roundTrip(
                new Message.DeleteRangeRequest("box", 3, "logs/", null, null));
        assertThat(byPrefix.partition()).isEqualTo(3);
        assertThat(byPrefix.prefix()).isEqualTo("logs/");
        assertThat(byPrefix.startKey()).isNull();

        Message.DeleteRangeRequest byWindow = (Message.DeleteRangeRequest) roundTrip(
                new Message.DeleteRangeRequest("box", 1, null, "b", "e"));
        assertThat(byWindow.prefix()).isNull();
        assertThat(byWindow.startKey()).isEqualTo("b");
        assertThat(byWindow.endKey()).isEqualTo("e");
    }

    @Test
    void listCandiesRequestRoundTripsRangeAndDirection() {
        Message.ListCandiesRequest req = new Message.ListCandiesRequest("box", 2, "p/", "p/cursor", 50,
                "p/a", "p/z", true);
        Message.ListCandiesRequest out = (Message.ListCandiesRequest) roundTrip(req);
        assertThat(out.partition()).isEqualTo(2);
        assertThat(out.startKey()).isEqualTo("p/a");
        assertThat(out.endKey()).isEqualTo("p/z");
        assertThat(out.reverse()).isTrue();
        assertThat(out.startAfter()).isEqualTo("p/cursor");
    }

    @Test
    void responsesRoundTrip() {
        assertThat(roundTrip(new Message.OkResponse())).isInstanceOf(Message.OkResponse.class);
        assertThat(roundTrip(new Message.NotFoundResponse())).isInstanceOf(Message.NotFoundResponse.class);

        Message.BusyResponse busy = (Message.BusyResponse) roundTrip(new Message.BusyResponse(250));
        assertThat(busy.retryAfterMillis()).isEqualTo(250);

        Message.ErrorResponse err = (Message.ErrorResponse) roundTrip(
                new Message.ErrorResponse("ValidationException", "bad key"));
        assertThat(err.errorType()).isEqualTo("ValidationException");
        assertThat(err.message()).isEqualTo("bad key");
    }

    @Test
    void listResponseRoundTrips() {
        Message.ListCandiesResponse resp = new Message.ListCandiesResponse(
                List.of(new Message.ListedCandy("a", 10, 100), new Message.ListedCandy("b", 20, 200)),
                "b");
        Message.ListCandiesResponse out = (Message.ListCandiesResponse) roundTrip(resp);
        assertThat(out.entries()).hasSize(2);
        assertThat(out.entries().get(1).key()).isEqualTo("b");
        assertThat(out.nextStartAfter()).isEqualTo("b");
    }

    @Test
    void headCandyResponseRoundTrips() {
        Message.HeadCandyResponse resp = new Message.HeadCandyResponse(1234, "image/png",
                Map.of("k", "v"), 0x55, 9000);
        Message.HeadCandyResponse out = (Message.HeadCandyResponse) roundTrip(resp);
        assertThat(out.contentLength()).isEqualTo(1234);
        assertThat(out.contentType()).isEqualTo("image/png");
        assertThat(out.userMetadata()).containsEntry("k", "v");
        assertThat(out.crc32c()).isEqualTo(0x55);
        assertThat(out.createdAtMillis()).isEqualTo(9000);
    }

    @Test
    void rangeGetCandyRequestRoundTrips() {
        Message.RangeGetCandyRequest req = new Message.RangeGetCandyRequest("box", "k", 6, 9);
        Message.RangeGetCandyRequest out = (Message.RangeGetCandyRequest) roundTrip(req);
        assertThat(out.firstByte()).isEqualTo(6);
        assertThat(out.lastByte()).isEqualTo(9);

        Message.RangeGetCandyRequest suffix = (Message.RangeGetCandyRequest) roundTrip(
                new Message.RangeGetCandyRequest("box", "k", -1, 3));
        assertThat(suffix.firstByte()).isEqualTo(-1);
        assertThat(suffix.lastByte()).isEqualTo(3);
    }

    @Test
    void candyDataResponseCarriesTotalLength() {
        Message.CandyDataResponse resp = new Message.CandyDataResponse(4, 14, "text/plain",
                Map.of(), 0x99, "cand".getBytes());
        Message.CandyDataResponse out = (Message.CandyDataResponse) roundTrip(resp);
        assertThat(out.contentLength()).isEqualTo(4);
        assertThat(out.totalLength()).isEqualTo(14);
        assertThat(new String(out.data())).isEqualTo("cand");
    }

    @Test
    void movedResponseRoundTrips() {
        Message.MovedResponse out = (Message.MovedResponse) roundTrip(new Message.MovedResponse(7));
        assertThat(out.ownerNodeId()).isEqualTo(7);
    }

    @Test
    void boxInfoRequestAndResponseRoundTrip() {
        Message.BoxInfoRequest req = (Message.BoxInfoRequest) roundTrip(
                new Message.BoxInfoRequest("my-box"));
        assertThat(req.box()).isEqualTo("my-box");
        assertThat(req.opcode()).isEqualTo(Opcode.BOX_INFO);

        Message.BoxInfoResponse resp = (Message.BoxInfoResponse) roundTrip(
                new Message.BoxInfoResponse(8));
        assertThat(resp.partitionCount()).isEqualTo(8);
        assertThat(resp.opcode()).isEqualTo(Opcode.RESPONSE_BOX_INFO);
    }

    @Test
    void listBoxesResponseRoundTrips() {
        Message.ListBoxesResponse out = (Message.ListBoxesResponse) roundTrip(
                new Message.ListBoxesResponse(List.of("alpha", "beta")));
        assertThat(out.boxes()).containsExactly("alpha", "beta");
    }

    @Test
    void boxRequestsRoundTrip() {
        Message.CreateBoxRequest create = (Message.CreateBoxRequest) roundTrip(
                new Message.CreateBoxRequest("my-box"));
        assertThat(create.box()).isEqualTo("my-box");
        assertThat(create.partitionCount()).isZero(); // convenience ctor = server default
        assertThat(create.opcode()).isEqualTo(Opcode.CREATE_BOX);

        Message.CreateBoxRequest partitioned = (Message.CreateBoxRequest) roundTrip(
                new Message.CreateBoxRequest("my-box", 16));
        assertThat(partitioned.partitionCount()).isEqualTo(16);

        Message.DeleteBoxRequest forced = (Message.DeleteBoxRequest) roundTrip(
                new Message.DeleteBoxRequest("my-box", true));
        assertThat(forced.box()).isEqualTo("my-box");
        assertThat(forced.force()).isTrue();

        Message.DeleteBoxRequest soft = (Message.DeleteBoxRequest) roundTrip(
                new Message.DeleteBoxRequest("my-box", false));
        assertThat(soft.force()).isFalse();

        assertThat(roundTrip(new Message.ListBoxesRequest())).isInstanceOf(Message.ListBoxesRequest.class);

        Message.HeadBoxRequest head = (Message.HeadBoxRequest) roundTrip(
                new Message.HeadBoxRequest("my-box"));
        assertThat(head.box()).isEqualTo("my-box");
    }

    @Test
    void candyKeyRequestsRoundTrip() {
        Message.GetCandyRequest get = (Message.GetCandyRequest) roundTrip(
                new Message.GetCandyRequest("box", "k/1"));
        assertThat(get.box()).isEqualTo("box");
        assertThat(get.key()).isEqualTo("k/1");

        Message.HeadCandyRequest headCandy = (Message.HeadCandyRequest) roundTrip(
                new Message.HeadCandyRequest("box", "k/2"));
        assertThat(headCandy.key()).isEqualTo("k/2");

        Message.DeleteCandyRequest delete = (Message.DeleteCandyRequest) roundTrip(
                new Message.DeleteCandyRequest("box", "k/3"));
        assertThat(delete.key()).isEqualTo("k/3");
    }

    @Test
    void candyDataResponseRoundTrips() {
        Message.CandyDataResponse resp = new Message.CandyDataResponse(
                7, "application/octet-stream", Map.of("x", "y"), 0xABCD, "candy".getBytes());
        Message.CandyDataResponse out = (Message.CandyDataResponse) roundTrip(resp);
        assertThat(out.contentLength()).isEqualTo(7);
        assertThat(out.contentType()).isEqualTo("application/octet-stream");
        assertThat(out.userMetadata()).containsEntry("x", "y");
        assertThat(out.crc32c()).isEqualTo(0xABCD);
        assertThat(new String(out.data())).isEqualTo("candy");
    }

    @Test
    void candyDataResponseHandlesNullContentTypeAndEmptyBody() {
        Message.CandyDataResponse resp = new Message.CandyDataResponse(
                0, null, Map.of(), 0, new byte[0]);
        Message.CandyDataResponse out = (Message.CandyDataResponse) roundTrip(resp);
        assertThat(out.contentType()).isNull();
        assertThat(out.userMetadata()).isEmpty();
        assertThat(out.data()).isEmpty();
    }

    @Test
    void plainListCandiesRequestConstructorRoundTrips() {
        Message.ListCandiesRequest req = new Message.ListCandiesRequest("box", 0, "p/", "p/x", 25);
        Message.ListCandiesRequest out = (Message.ListCandiesRequest) roundTrip(req);
        assertThat(out.prefix()).isEqualTo("p/");
        assertThat(out.startAfter()).isEqualTo("p/x");
        assertThat(out.maxKeys()).isEqualTo(25);
        assertThat(out.startKey()).isNull();
        assertThat(out.endKey()).isNull();
        assertThat(out.reverse()).isFalse();
    }

    @Test
    void multipartCreateAndUploadRequestsRoundTrip() {
        Message.CreateMultipartUploadRequest create =
                (Message.CreateMultipartUploadRequest) roundTrip(new Message.CreateMultipartUploadRequest(
                        "box", "big", "text/plain", Map.of("k", "v")));
        assertThat(create.key()).isEqualTo("big");
        assertThat(create.contentType()).isEqualTo("text/plain");
        assertThat(create.userMetadata()).containsEntry("k", "v");
        assertThat(create.opcode()).isEqualTo(Opcode.CREATE_MULTIPART_UPLOAD);

        // Null content-type should survive the round trip.
        Message.CreateMultipartUploadRequest noType =
                (Message.CreateMultipartUploadRequest) roundTrip(new Message.CreateMultipartUploadRequest(
                        "box", "big", null, Map.of()));
        assertThat(noType.contentType()).isNull();
        assertThat(noType.userMetadata()).isEmpty();

        Message.UploadPartRequest part = (Message.UploadPartRequest) roundTrip(
                new Message.UploadPartRequest("box", "big", "upload-1", 3, "chunk".getBytes()));
        assertThat(part.uploadId()).isEqualTo("upload-1");
        assertThat(part.partNumber()).isEqualTo(3);
        assertThat(new String(part.data())).isEqualTo("chunk");

        Message.AbortMultipartUploadRequest abort = (Message.AbortMultipartUploadRequest) roundTrip(
                new Message.AbortMultipartUploadRequest("box", "big", "upload-1"));
        assertThat(abort.uploadId()).isEqualTo("upload-1");
    }

    @Test
    void completeMultipartUploadRequestRoundTripsPartList() {
        Message.CompleteMultipartUploadRequest req = new Message.CompleteMultipartUploadRequest(
                "box", "big", "upload-7",
                List.of(new Message.CompletedPart(1, 0x11), new Message.CompletedPart(2, 0x22)),
                "idem-9");
        Message.CompleteMultipartUploadRequest out =
                (Message.CompleteMultipartUploadRequest) roundTrip(req);
        assertThat(out.uploadId()).isEqualTo("upload-7");
        assertThat(out.idempotencyToken()).isEqualTo("idem-9");
        assertThat(out.parts()).hasSize(2);
        assertThat(out.parts().get(1).partNumber()).isEqualTo(2);
        assertThat(out.parts().get(1).crc32c()).isEqualTo(0x22);

        // Null idempotency token and empty part list also round-trip.
        Message.CompleteMultipartUploadRequest empty =
                (Message.CompleteMultipartUploadRequest) roundTrip(new Message.CompleteMultipartUploadRequest(
                        "box", "big", "upload-7", List.of(), null));
        assertThat(empty.parts()).isEmpty();
        assertThat(empty.idempotencyToken()).isNull();
    }

    @Test
    void uploadPartCopyRequestRoundTripsRangeBounds() {
        Message.UploadPartCopyRequest copy = (Message.UploadPartCopyRequest) roundTrip(
                new Message.UploadPartCopyRequest("box", "dst", "upload-2", 4, "src", 10, 99));
        assertThat(copy.srcKey()).isEqualTo("src");
        assertThat(copy.partNumber()).isEqualTo(4);
        assertThat(copy.firstByte()).isEqualTo(10);
        assertThat(copy.lastByte()).isEqualTo(99);

        // Open-ended copy (whole source) encoded as (-1, -1).
        Message.UploadPartCopyRequest whole = (Message.UploadPartCopyRequest) roundTrip(
                new Message.UploadPartCopyRequest("box", "dst", "upload-2", 1, "src", -1, -1));
        assertThat(whole.firstByte()).isEqualTo(-1);
        assertThat(whole.lastByte()).isEqualTo(-1);
    }

    @Test
    void multipartListingRequestsRoundTrip() {
        Message.ListMultipartUploadsRequest uploads =
                (Message.ListMultipartUploadsRequest) roundTrip(new Message.ListMultipartUploadsRequest(
                        "box", 4, "p/", "key-m", "upload-m", 200));
        assertThat(uploads.partition()).isEqualTo(4);
        assertThat(uploads.prefix()).isEqualTo("p/");
        assertThat(uploads.keyMarker()).isEqualTo("key-m");
        assertThat(uploads.uploadIdMarker()).isEqualTo("upload-m");
        assertThat(uploads.maxUploads()).isEqualTo(200);

        Message.ListPartsRequest parts = (Message.ListPartsRequest) roundTrip(
                new Message.ListPartsRequest("box", "big", "upload-3", 5, 100));
        assertThat(parts.uploadId()).isEqualTo("upload-3");
        assertThat(parts.partNumberMarker()).isEqualTo(5);
        assertThat(parts.maxParts()).isEqualTo(100);
    }

    @Test
    void multipartResponsesRoundTrip() {
        Message.CreateMultipartUploadResponse created =
                (Message.CreateMultipartUploadResponse) roundTrip(
                        new Message.CreateMultipartUploadResponse("upload-42"));
        assertThat(created.uploadId()).isEqualTo("upload-42");

        Message.UploadPartResponse part = (Message.UploadPartResponse) roundTrip(
                new Message.UploadPartResponse(0xDEAD, 4096));
        assertThat(part.crc32c()).isEqualTo(0xDEAD);
        assertThat(part.partLength()).isEqualTo(4096);

        Message.ListMultipartUploadsResponse uploads =
                (Message.ListMultipartUploadsResponse) roundTrip(new Message.ListMultipartUploadsResponse(
                        List.of(new Message.InProgressUpload("u1", "a", 100),
                                new Message.InProgressUpload("u2", "b", 200)),
                        "next-key", "next-upload"));
        assertThat(uploads.uploads()).hasSize(2);
        assertThat(uploads.uploads().get(0).uploadId()).isEqualTo("u1");
        assertThat(uploads.nextKeyMarker()).isEqualTo("next-key");
        assertThat(uploads.nextUploadIdMarker()).isEqualTo("next-upload");

        Message.ListPartsResponse parts = (Message.ListPartsResponse) roundTrip(
                new Message.ListPartsResponse(
                        List.of(new Message.UploadedPart(1, 1024, 0x01),
                                new Message.UploadedPart(2, 2048, 0x02)),
                        7));
        assertThat(parts.parts()).hasSize(2);
        assertThat(parts.parts().get(1).partNumber()).isEqualTo(2);
        assertThat(parts.parts().get(1).partLength()).isEqualTo(2048);
        assertThat(parts.nextPartNumberMarker()).isEqualTo(7);
    }

    @Test
    void saslMessagesRoundTrip() {
        Message.SaslHandshakeRequest handshake = (Message.SaslHandshakeRequest) roundTrip(
                new Message.SaslHandshakeRequest("SCRAM-SHA-256"));
        assertThat(handshake.mechanism()).isEqualTo("SCRAM-SHA-256");

        Message.SaslHandshakeResponse handshakeResponse =
                (Message.SaslHandshakeResponse) roundTrip(
                        new Message.SaslHandshakeResponse(false, List.of("PLAIN", "SCRAM-SHA-256")));
        assertThat(handshakeResponse.ok()).isFalse();
        assertThat(handshakeResponse.enabledMechanisms())
                .containsExactly("PLAIN", "SCRAM-SHA-256");

        byte[] token = {0, 1, 2, (byte) 0xFF};
        Message.SaslAuthenticateRequest authenticate = (Message.SaslAuthenticateRequest) roundTrip(
                new Message.SaslAuthenticateRequest(token));
        assertThat(authenticate.token()).isEqualTo(token);
        assertThat(((Message.SaslAuthenticateRequest) roundTrip(
                new Message.SaslAuthenticateRequest(null))).token()).isEmpty();

        Message.SaslAuthenticateResponse challenge = (Message.SaslAuthenticateResponse) roundTrip(
                new Message.SaslAuthenticateResponse(true, token));
        assertThat(challenge.complete()).isTrue();
        assertThat(challenge.challenge()).isEqualTo(token);

        Message.AuthFailedResponse failed = (Message.AuthFailedResponse) roundTrip(
                new Message.AuthFailedResponse("Authentication failed"));
        assertThat(failed.message()).isEqualTo("Authentication failed");
    }
}
