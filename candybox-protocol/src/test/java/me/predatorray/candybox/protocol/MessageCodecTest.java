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
}
