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
package me.predatorray.candybox.lsm.manifest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.SegmentRef;
import org.junit.jupiter.api.Test;

/**
 * Validation and defensive-copy coverage for the manifest edit value types: {@link ManifestEdit},
 * its {@link ManifestEdit.PartUpsert}, the {@link ManifestEdit.Builder} setters, and
 * {@link MultipartUploadState}.
 */
class ManifestEditTest {

    private static Part part() {
        return new Part(3, 16, 0, List.of(new SegmentRef(1, 0, 0)));
    }

    @Test
    void constructorNormalizesNullCollectionsToEmpty() {
        ManifestEdit edit = new ManifestEdit(List.of(), Set.of(), Set.of(), Set.of(), null,
                null, null, null, 0L);
        assertThat(edit.addedUploads()).isEmpty();
        assertThat(edit.upsertParts()).isEmpty();
        assertThat(edit.removedUploads()).isEmpty();
    }

    @Test
    void constructorRejectsNegativeFencingToken() {
        assertThatThrownBy(() -> new ManifestEdit(List.of(), Set.of(), Set.of(), Set.of(), null,
                List.of(), List.of(), Set.of(), -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ownerFencingToken");
    }

    @Test
    void partUpsertValidatesItsFields() {
        assertThatThrownBy(() -> new ManifestEdit.PartUpsert("", 1, part()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uploadId");
        assertThatThrownBy(() -> new ManifestEdit.PartUpsert("u", 0, part()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partNumber");
        assertThatThrownBy(() -> new ManifestEdit.PartUpsert("u", 1, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("part is required");
    }

    @Test
    void builderSettersPopulateUploadAndPartLists() {
        MultipartUploadState upload = new MultipartUploadState("up", "key", null, Map.of(), 1L,
                Map.of());
        ManifestEdit edit = ManifestEdit.builder()
                .addedUploads(List.of(upload))
                .upsertParts(List.of(new ManifestEdit.PartUpsert("up", 1, part())))
                .removedUploads(Set.of("gone"))
                .ownerFencingToken(7L)
                .build();
        assertThat(edit.addedUploads()).extracting(MultipartUploadState::uploadId).containsExactly("up");
        assertThat(edit.upsertParts()).hasSize(1);
        assertThat(edit.removedUploads()).containsExactly("gone");
        assertThat(edit.ownerFencingToken()).isEqualTo(7L);
    }

    @Test
    void multipartUploadStateValidatesIdAndKey() {
        assertThatThrownBy(() -> new MultipartUploadState("", "key", null, Map.of(), 0L, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uploadId");
        assertThatThrownBy(() -> new MultipartUploadState("up", "", null, Map.of(), 0L, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key");
    }

    @Test
    void multipartUploadStateNormalizesNullMetadataAndSortsParts() {
        MultipartUploadState state = new MultipartUploadState("up", "key", null, null, 0L,
                Map.of(2, part(), 1, part()));
        assertThat(state.userMetadata()).isEmpty();
        assertThat(state.parts().keySet()).containsExactly(1, 2); // sorted ascending

        // withPart installs a new part number and keeps ordering.
        MultipartUploadState withThree = state.withPart(3, part());
        assertThat(withThree.parts().keySet()).containsExactly(1, 2, 3);
        assertThatThrownBy(() -> state.withPart(0, part()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partNumber");
    }
}
