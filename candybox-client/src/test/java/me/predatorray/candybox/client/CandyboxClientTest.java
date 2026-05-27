package me.predatorray.candybox.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.LoopbackTransport;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link CandyboxClient} against a canned {@link RequestHandler} over the in-JVM
 * {@link LoopbackTransport}, exercising the new WS4 responses (HEAD, listBoxes, MOVED) end to end
 * through the real codec without a server or sockets.
 */
class CandyboxClientTest {

    private static final MessageCodec CODEC = new MessageCodec();

    /** A stub node: HEAD/listBoxes/headBox canned, and box "moved-box" reports a MOVED owner. */
    private static final RequestHandler HANDLER = request -> {
        Message message = CODEC.decode(request);
        Message response;
        if (message instanceof Message.HeadCandyRequest) {
            response = new Message.HeadCandyResponse(42, "text/plain", Map.of("a", "b"), 7, 123);
        } else if (message instanceof Message.ListBoxesRequest) {
            response = new Message.ListBoxesResponse(List.of("alpha", "beta"));
        } else if (message instanceof Message.HeadBoxRequest hb) {
            response = hb.box().equals("exists-box")
                    ? new Message.OkResponse() : new Message.NotFoundResponse();
        } else if (message instanceof Message.GetCandyRequest g && g.box().equals("moved-box")) {
            response = new Message.MovedResponse(9);
        } else {
            response = new Message.OkResponse();
        }
        return CODEC.encode(response);
    };

    private CandyboxClient client() {
        return new CandyboxClient(new LoopbackTransport(HANDLER), "ignored", 0);
    }

    @Test
    void headCandyDecodesMetadata() {
        try (CandyboxClient client = client()) {
            CandyboxClient.CandyInfo info = client.headCandy("my-box", "k");
            assertThat(info.contentLength()).isEqualTo(42);
            assertThat(info.contentType()).isEqualTo("text/plain");
            assertThat(info.userMetadata()).containsEntry("a", "b");
            assertThat(info.crc32c()).isEqualTo(7);
            assertThat(info.createdAtMillis()).isEqualTo(123);
        }
    }

    @Test
    void listBoxesAndHeadBox() {
        try (CandyboxClient client = client()) {
            assertThat(client.listBoxes()).containsExactly("alpha", "beta");
            assertThat(client.headBox("exists-box")).isTrue();
            assertThat(client.headBox("ghost-box")).isFalse();
        }
    }

    @Test
    void movedResponseSurfacesAsNotOwner() {
        try (CandyboxClient client = client()) {
            assertThatThrownBy(() -> client.getCandy("moved-box", "k"))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("node 9");
        }
    }
}
