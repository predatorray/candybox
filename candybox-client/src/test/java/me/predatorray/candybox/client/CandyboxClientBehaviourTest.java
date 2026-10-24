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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.common.exception.StorageException;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.LoopbackTransport;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.junit.jupiter.api.Test;

/**
 * Public-API behaviours of {@link CandyboxClient} (error mapping, stream overloads) driven through
 * canned handlers; {@code candybox-server} is not visible from this module.
 */
class CandyboxClientBehaviourTest {

    private static final MessageCodec CODEC = new MessageCodec();

    /** Answers BoxInfo itself (keyed calls look up the partition count) and defers the rest. */
    private static CandyboxClient client(java.util.function.Function<Message, Message> node) {
        RequestHandler handler = request -> {
            Message message = CODEC.decode(request);
            Message response = message instanceof Message.BoxInfoRequest
                    ? new Message.BoxInfoResponse(1) : node.apply(message);
            return CODEC.encode(response);
        };
        return new CandyboxClient(new LoopbackTransport(handler), "ignored", 0);
    }

    @Test
    void getBoxAclOnMissingBoxThrowsBoxNotFound() {
        try (CandyboxClient client = client(m -> new Message.NotFoundResponse())) {
            assertThatThrownBy(() -> client.getBoxAcl("no-such-box"))
                    .isInstanceOf(BoxNotFoundException.class);
        }
    }

    @Test
    void getBoxAclOnLegacyBoxIsEmpty() {
        try (CandyboxClient client = client(m -> m instanceof Message.HeadBoxRequest
                ? new Message.OkResponse() : new Message.NotFoundResponse())) {
            assertThat(client.getBoxAcl("legacy-box")).isEmpty();
        }
    }

    @Test
    void getBoxAclDecodesOwnerAndGrants() {
        try (CandyboxClient client = client(m -> new Message.BoxAclResponse("User:alice",
                List.of("User:bob:READ")))) {
            BoxAcl acl = client.getBoxAcl("acl-box").orElseThrow();
            assertThat(acl.owner().toString()).isEqualTo("User:alice");
            assertThat(acl.grants()).extracting(g -> g.toText()).containsExactly("User:bob:READ");
        }
    }

    @Test
    void listPartsOfUnknownUploadThrowsCandyNotFound() {
        try (CandyboxClient client = client(m -> new Message.NotFoundResponse())) {
            assertThatThrownBy(() -> client.listParts("my-box", "k", "nope", 0, 100))
                    .isInstanceOf(CandyNotFoundException.class);
        }
    }

    @Test
    void singleNodeMovedSurfacesAsNotOwner() {
        try (CandyboxClient client = client(m -> new Message.MovedResponse(7))) {
            assertThatThrownBy(() -> client.getCandy("my-box", "k"))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("node 7");
        }
    }

    @Test
    void singleNodeUnavailableSurfacesAsNotOwner() {
        try (CandyboxClient client = client(m -> new Message.UnavailableResponse(100))) {
            assertThatThrownBy(() -> client.getCandy("my-box", "k"))
                    .isInstanceOf(NotOwnerException.class)
                    .hasMessageContaining("no current owner");
        }
    }

    @Test
    void busyResponseMapsToBusyException() {
        try (CandyboxClient client = client(m -> new Message.BusyResponse(250))) {
            assertThatThrownBy(() -> client.getCandy("my-box", "k"))
                    .isInstanceOf(BusyException.class)
                    .hasMessageContaining("250");
        }
    }

    @Test
    void streamingPutSendsExactlyTheStreamBytes() {
        AtomicReference<byte[]> sent = new AtomicReference<>();
        try (CandyboxClient client = client(m -> {
            if (m instanceof Message.PutCandyRequest put) {
                sent.set(put.data());
            }
            return new Message.OkResponse();
        })) {
            byte[] bytes = "hello streaming".getBytes(StandardCharsets.UTF_8);
            client.putCandy("my-box", "k", new ByteArrayInputStream(bytes), "text/plain", Map.of(),
                    null);
            assertThat(sent.get()).isEqualTo(bytes);
        }
    }

    @Test
    void streamingGetSurfacesOutputFailureAsStorageException() {
        byte[] data = {1, 2, 3};
        try (CandyboxClient client = client(m -> new Message.CandyDataResponse(3, 3,
                "application/octet-stream", Map.of(), 0, data))) {
            OutputStream failing = new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    throw new IOException("disk full");
                }
            };
            assertThatThrownBy(() -> client.getCandy("my-box", "k", failing))
                    .isInstanceOf(StorageException.class)
                    .hasMessage("Failed writing Candy to output stream")
                    .hasCauseInstanceOf(IOException.class);
        }
    }
}
