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
package me.predatorray.candybox.lsm.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.ValidationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

// uploadPartCopy reports typed InvalidRange validation errors in the current engine;
// IllegalArgumentException applies to getCandyRange, not this multipart write path.
class BoxEngineUploadPartCopyRangeTest {
    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final BoxEngine engine = BoxEngine.createNew(BoxName.of("range-box"),
            CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);
    private final CandyKey source = CandyKey.of("src");
    private final CandyKey destination = CandyKey.of("dst");

    @AfterEach
    void close() {
        engine.close();
        store.close();
    }

    private String upload(byte[] data) {
        engine.putCandy(source, data, null, Map.of(), null);
        return engine.createMultipartUpload(destination, null, Map.of());
    }

    @Test
    void reversedRangeIsRejected() {
        String upload = upload(new byte[10]);
        assertThatThrownBy(() -> engine.uploadPartCopy(upload, 1, source, 5, 2))
                .isInstanceOf(ValidationException.class).hasMessageContaining("InvalidRange")
                .hasMessageContaining("not satisfiable");
        assertThat(engine.multipartUpload(upload).parts()).isEmpty();
    }

    @Test
    void firstByteAtSourceLengthIsNotSatisfiable() {
        String upload = upload(new byte[10]);
        assertThatThrownBy(() -> engine.uploadPartCopy(upload, 1, source, 10, 12))
                .isInstanceOf(ValidationException.class).hasMessageContaining("InvalidRange")
                .hasMessageContaining("not satisfiable");
        assertThat(engine.multipartUpload(upload).parts()).isEmpty();
    }

    @Test
    void lastByteBeyondSourceIsClamped() {
        String upload = upload(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        var part = engine.uploadPartCopy(upload, 1, source, 3, 100);
        assertThat(part.partLength()).isEqualTo(7);
        engine.completeMultipartUpload(upload,
                List.of(new BoxEngine.PartCompletion(1, part.crc32c())), null);
        assertThat(engine.getCandy(destination)).containsExactly(3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    void explicitRangeOnEmptySourceIsNotSatisfiable() {
        String upload = upload(new byte[0]);
        assertThatThrownBy(() -> engine.uploadPartCopy(upload, 1, source, 0, 1))
                .isInstanceOf(ValidationException.class).hasMessageContaining("InvalidRange")
                .hasMessageContaining("not satisfiable");
        assertThat(engine.multipartUpload(upload).parts()).isEmpty();
    }
}
