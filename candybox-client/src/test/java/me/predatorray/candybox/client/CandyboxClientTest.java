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
        } else if (message instanceof Message.CopyCandyRequest
                || message instanceof Message.RenameCandyRequest) {
            response = new Message.HeadCandyResponse(5, "text/plain", Map.of(), 9, 1);
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
    void copyRenameAndDeleteRangeRoundTripThroughTheCodec() {
        try (CandyboxClient client = client()) {
            CandyboxClient.CandyInfo copied = client.copyCandy("my-box", "src", "dst", null);
            assertThat(copied.contentLength()).isEqualTo(5);
            CandyboxClient.CandyInfo renamed = client.renameCandy("my-box", "src", "dst", null);
            assertThat(renamed.crc32c()).isEqualTo(9);
            // delete-range forms return OK; assert they encode/route without error.
            client.deleteRangeByPrefix("my-box", "logs/");
            client.deleteRange("my-box", "a", "m");
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
