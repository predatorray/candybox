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

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class S3XmlFieldsTest {
    @Test
    void v2EchoesStartAfter() {
        assertThat(S3Xml.listBucketV2("photos", null, null, 1000, List.of(), List.of(),
                null, null, "k")).contains("<StartAfter>k</StartAfter>");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bothListingVersionsQuoteEtags(boolean v2) {
        List<S3Xml.Content> contents = List.of(new S3Xml.Content("key", 4, 0, "abc123"));
        String xml = v2
                ? S3Xml.listBucketV2("photos", null, null, 1000, contents, List.of(), null, null, null)
                : S3Xml.listBucketV1("photos", null, null, 1000, contents, List.of(), null, null, false);
        assertThat(xml).contains("<Key>key</Key>").contains("<ETag>\"abc123\"</ETag>");
    }

    @Test
    void versionListingQuotesEtags() {
        assertThat(S3Xml.listVersions("photos", null, null, 1000,
                List.of(new S3Xml.Content("key", 4, 0, "abc123")), List.of(), null, null))
                .contains("<Version>").contains("<ETag>\"abc123\"</ETag>");
    }

    @Test
    void locationConstraintIncludesNonDefaultRegionOnly() {
        assertThat(S3Xml.locationConstraint("eu-west-1"))
                .contains("<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">eu-west-1</LocationConstraint>");
        assertThat(S3Xml.locationConstraint("us-east-1"))
                .contains("></LocationConstraint>").doesNotContain("us-east-1");
    }
}
