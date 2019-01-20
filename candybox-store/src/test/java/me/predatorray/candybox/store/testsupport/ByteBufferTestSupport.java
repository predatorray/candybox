/*
 * Copyright (c) 2019 the original author or authors.
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

package me.predatorray.candybox.store.testsupport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class ByteBufferTestSupport {

    public static byte[] toByteArray(List<ByteBuffer> byteBuffers) {
        byte[] actualData;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             WritableByteChannel baosChannel = Channels.newChannel(baos)) {
            for (ByteBuffer byteBuffer : byteBuffers) {
                long remaining = byteBuffer.remaining();
                while (remaining > 0) {
                    remaining -= baosChannel.write(byteBuffer);
                }
            }
            baos.flush();
            actualData = baos.toByteArray();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex); // io exception is not expected
        }
        return actualData;
    }
}
