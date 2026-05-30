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

class S3XmlTest {

    @Test
    void errorBody() {
        String xml = S3Xml.error(S3ErrorCode.NO_SUCH_KEY, "nope", "/b/k", "req-1");
        assertThat(xml).contains("<Code>NoSuchKey</Code>")
                .contains("<Message>nope</Message>")
                .contains("<Resource>/b/k</Resource>")
                .contains("<RequestId>req-1</RequestId>");
    }

    @Test
    void listAllMyBuckets() {
        String xml = S3Xml.listAllMyBuckets(List.of("photos", "videos"));
        assertThat(xml).contains("<Name>photos</Name>").contains("<Name>videos</Name>")
                .contains(S3Xml.NS);
    }

    @Test
    void listBucketTruncationAndContents() {
        String xml = S3Xml.listBucket("photos", "a/", "/", 1000,
                List.of(new S3Xml.Content("a/cat.jpg", 42, 0L, null)),
                List.of("a/sub/"), null, "TOKEN", null);
        assertThat(xml).contains("<Key>a/cat.jpg</Key>")
                .contains("<Size>42</Size>")
                .contains("<CommonPrefixes><Prefix>a/sub/</Prefix></CommonPrefixes>")
                .contains("<IsTruncated>true</IsTruncated>")
                .contains("<NextContinuationToken>TOKEN</NextContinuationToken>");
    }

    @Test
    void notTruncatedWhenNoNextToken() {
        String xml = S3Xml.listBucket("photos", "", null, 1000, List.of(), List.of(), null, null, null);
        assertThat(xml).contains("<IsTruncated>false</IsTruncated>")
                .doesNotContain("NextContinuationToken");
    }

    @Test
    void escapesSpecialCharactersInKeys() {
        String xml = S3Xml.listBucket("photos", "", null, 1000,
                List.of(new S3Xml.Content("a & b <c>.jpg", 1, 0L, null)), List.of(), null, null, null);
        assertThat(xml).contains("a &amp; b &lt;c&gt;.jpg").doesNotContain("a & b <c>");
    }

    @Test
    void locationConstraintEmptyForUsEast1() {
        assertThat(S3Xml.locationConstraint("us-east-1")).contains("<LocationConstraint")
                .doesNotContain("us-east-1<");
        assertThat(S3Xml.locationConstraint("eu-west-1")).contains(">eu-west-1<");
    }

    @Test
    void copyObjectResultQuotesEtag() {
        String xml = S3Xml.copyObjectResult("abc123", 0L);
        assertThat(xml).contains("<CopyObjectResult>")
                .contains("<ETag>\"abc123\"</ETag>")
                .contains("<LastModified>");
    }

    @Test
    void deleteResultListsDeletedAndErrors() {
        String xml = S3Xml.deleteResult(List.of("a.txt"),
                List.<String[]>of(new String[]{"b.txt", "AccessDenied", "nope"}));
        assertThat(xml).contains("<Deleted><Key>a.txt</Key></Deleted>")
                .contains("<Error><Key>b.txt</Key><Code>AccessDenied</Code><Message>nope</Message></Error>");
    }

    @Test
    void versioningAndAclAreWellFormed() {
        assertThat(S3Xml.versioningConfiguration()).contains("VersioningConfiguration").contains(S3Xml.NS);
        String acl = S3Xml.accessControlPolicy();
        assertThat(acl).contains("<AccessControlPolicy")
                .contains("<Permission>FULL_CONTROL</Permission>")
                .contains("xsi:type=\"CanonicalUser\"");
    }

    @Test
    void declaresXmlPrologue() {
        assertThat(S3Xml.error(S3ErrorCode.INTERNAL_ERROR, null, null, "r"))
                .startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    }

    @Test
    void errorUsesDefaultMessageWhenNull() {
        assertThat(S3Xml.error(S3ErrorCode.NO_SUCH_BUCKET, null, null, "r"))
                .contains("<Message>" + S3ErrorCode.NO_SUCH_BUCKET.defaultMessage() + "</Message>");
    }
}
