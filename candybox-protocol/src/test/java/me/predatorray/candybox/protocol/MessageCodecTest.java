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
                new Message.DeleteRangeRequest("box", "logs/", null, null));
        assertThat(byPrefix.prefix()).isEqualTo("logs/");
        assertThat(byPrefix.startKey()).isNull();

        Message.DeleteRangeRequest byWindow = (Message.DeleteRangeRequest) roundTrip(
                new Message.DeleteRangeRequest("box", null, "b", "e"));
        assertThat(byWindow.prefix()).isNull();
        assertThat(byWindow.startKey()).isEqualTo("b");
        assertThat(byWindow.endKey()).isEqualTo("e");
    }

    @Test
    void listCandiesRequestRoundTripsRangeAndDirection() {
        Message.ListCandiesRequest req = new Message.ListCandiesRequest("box", "p/", "p/cursor", 50,
                "p/a", "p/z", true);
        Message.ListCandiesRequest out = (Message.ListCandiesRequest) roundTrip(req);
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
    void movedResponseRoundTrips() {
        Message.MovedResponse out = (Message.MovedResponse) roundTrip(new Message.MovedResponse(7));
        assertThat(out.ownerNodeId()).isEqualTo(7);
    }

    @Test
    void listBoxesResponseRoundTrips() {
        Message.ListBoxesResponse out = (Message.ListBoxesResponse) roundTrip(
                new Message.ListBoxesResponse(List.of("alpha", "beta")));
        assertThat(out.boxes()).containsExactly("alpha", "beta");
    }
}
