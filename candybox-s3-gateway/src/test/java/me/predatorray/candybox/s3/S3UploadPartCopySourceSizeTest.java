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
package me.predatorray.candybox.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class S3UploadPartCopySourceSizeTest {
    private static final int FIVE_MIB = 5 * 1024 * 1024;
    private final FakeCandyStore store = new FakeCandyStore();

    @ParameterizedTest
    @ValueSource(ints = {0, 10, FIVE_MIB - 1, FIVE_MIB})
    void rangeCopyRequiresSourceLargerThanFiveMib(int sourceSize) {
        String uploadId = prepare(sourceSize);
        // Reject unsupported sources before checking whether the byte range is satisfiable.
        for (String range : new String[] {"bytes=2-5", "bytes=10000000-10000001"}) {
            Response response = copy(uploadId, range);
            assertThat(response.status()).isEqualTo(400);
            assertThat(response.body()).contains("<Code>InvalidRequest</Code>");
            assertThat(store.listParts("bucket", "target", uploadId, 0, 1000).parts()).isEmpty();
        }
    }

    @Test
    void sourceOneByteAboveFiveMibSupportsRangeCopy() {
        String uploadId = prepare(FIVE_MIB + 1);
        Response response = copy(uploadId, "bytes=2-5");
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).contains("<CopyPartResult ");
        var parts = store.listParts("bucket", "target", uploadId, 0, 1000).parts();
        assertThat(parts).hasSize(1);
        assertThat(parts.get(0).partLength()).isEqualTo(4);
        store.completeMultipartUpload("bucket", "target", uploadId, parts);
        assertThat(store.getCandy("bucket", "target")).containsExactly((byte) 2, (byte) 3, (byte) 4, (byte) 5);
    }

    @Test
    void supportedSourceRetainsUnsatisfiableRangeError() {
        String uploadId = prepare(FIVE_MIB + 1);
        Response response = copy(uploadId, "bytes=10000000-10000001");
        assertThat(response.status()).isEqualTo(416);
        assertThat(response.body()).contains("<Code>InvalidRange</Code>");
    }

    @Test
    void wholeObjectCopyAllowsSmallSource() {
        String uploadId = prepare(10);
        assertThat(copy(uploadId, null).status()).isEqualTo(200);
        var parts = store.listParts("bucket", "target", uploadId, 0, 1000).parts();
        store.completeMultipartUpload("bucket", "target", uploadId, parts);
        assertThat(store.getCandy("bucket", "target")).isEqualTo(store.getCandy("bucket", "source"));
    }

    @Test
    void missingRangeSourceRetainsNoSuchKeyError() {
        String uploadId = prepare(10);
        store.deleteCandy("bucket", "source");
        Response response = copy(uploadId, "bytes=2-5");
        assertThat(response.status()).isEqualTo(404);
        assertThat(response.body()).contains("<Code>NoSuchKey</Code>");
    }

    private String prepare(int sourceSize) {
        store.createBox("bucket");
        byte[] data = new byte[sourceSize];
        for (int i = 0; i < Math.min(10, sourceSize); i++) {
            data[i] = (byte) i;
        }
        store.putCandy("bucket", "source", data, "application/octet-stream", Map.of());
        return store.createMultipartUpload("bucket", "target", "application/octet-stream", Map.of());
    }

    private Response copy(String uploadId, String range) {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "unused:2181");
        EmbeddedChannel channel = new EmbeddedChannel(new S3Handler(store,
                S3GatewayConfig.fromProperties(properties, Map.of())));
        try {
            var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT,
                    "/bucket/target?partNumber=1&uploadId=" + uploadId);
            request.headers().set("x-amz-copy-source", "/bucket/source");
            if (range != null) {
                request.headers().set("x-amz-copy-source-range", range);
            }
            channel.writeInbound(request);
            FullHttpResponse response = channel.readOutbound();
            try {
                return new Response(response.status().code(), response.content().toString(StandardCharsets.UTF_8));
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private record Response(int status, String body) { }
}
