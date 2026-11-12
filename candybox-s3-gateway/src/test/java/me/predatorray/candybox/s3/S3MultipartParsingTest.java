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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class S3MultipartParsingTest {

    @ParameterizedTest
    @ValueSource(strings = {"0a1b2c3d", "\"0a1b2c3d\"", "\"0a1b2c3d-3\""})
    void acceptsQuotedUnquotedAndMultipartEtags(String etag) {
        assertThat(Etag.parseCrc32cHex(etag)).isEqualTo(0x0a1b2c3d);
    }

    // Assert local parser rejection only: AWS completion error mapping is tracked separately.
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"1234567", "\"zzzzzzzz\""})
    void rejectsMissingShortAndNonHexEtags(String etag) {
        assertThatThrownBy(() -> Etag.parseCrc32cHex(etag))
                .isInstanceOfSatisfying(S3Exception.class, e -> {
                    assertThat(e).hasMessageContaining(etag == null ? "Missing ETag" : "Malformed ETag");
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "<CompleteMultipartUpload><Part><ETag>0a1b2c3d</ETag></Part></CompleteMultipartUpload>",
            "<CompleteMultipartUpload/>",
            "<CompleteMultipartUpload><Part><PartNumber>abc</PartNumber></Part></CompleteMultipartUpload>",
            "not XML"
    })
    void rejectsMalformedCompletionBodies(String xml) {
        assertThatThrownBy(() -> S3RequestXml.parseCompleteMultipart(xml.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOfSatisfying(S3Exception.class,
                        e -> assertThat(e.error()).isEqualTo(S3ErrorCode.MALFORMED_XML));
    }

    @Test
    void invalidPartNumberHasUsefulMessage() {
        String xml = "<CompleteMultipartUpload><Part><PartNumber>abc</PartNumber>"
                + "</Part></CompleteMultipartUpload>";
        assertThatThrownBy(() -> S3RequestXml.parseCompleteMultipart(xml.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(S3Exception.class).hasMessageContaining("Invalid PartNumber");
    }

    @Test
    void uploadListingEchoesMarkersPrefixAndContinuation() {
        String xml = S3Xml.listMultipartUploadsResult("photos", "folder/", "folder/a", "upload-a",
                2, true, "folder/c", "upload-c",
                List.of(new S3Xml.UploadRow("folder/c", "upload-c", 0)));
        assertThat(xml).contains("<KeyMarker>folder/a</KeyMarker>",
                "<UploadIdMarker>upload-a</UploadIdMarker>", "<Prefix>folder/</Prefix>",
                "<NextKeyMarker>folder/c</NextKeyMarker>",
                "<NextUploadIdMarker>upload-c</NextUploadIdMarker>", "<IsTruncated>true</IsTruncated>");
    }

    @Test
    void finalUploadPageOmitsContinuationMarkers() {
        String xml = S3Xml.listMultipartUploadsResult("photos", "", null, null,
                1000, false, null, null, List.of());
        assertThat(xml).contains("<IsTruncated>false</IsTruncated>")
                .doesNotContain("<NextKeyMarker>", "<NextUploadIdMarker>", "<Prefix>");
    }

    @Test
    void partsListingEmitsContinuationOnlyWhenTruncated() {
        assertThat(S3Xml.listPartsResult("photos", "object", "upload", 1, 2, true, 3, List.of()))
                .contains("<PartNumberMarker>1</PartNumberMarker>",
                        "<NextPartNumberMarker>3</NextPartNumberMarker>", "<IsTruncated>true</IsTruncated>");
        assertThat(S3Xml.listPartsResult("photos", "object", "upload", 3, 2, false, 0, List.of()))
                .contains("<IsTruncated>false</IsTruncated>").doesNotContain("<NextPartNumberMarker>");
    }
}
