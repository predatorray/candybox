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

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies the SigV4 primitives against the worked example published in the AWS Signature
 * Version 4 documentation ({@code GET https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08}
 * with the well-known {@code AKIDEXAMPLE} credentials) — interoperability evidence that the
 * canonicalization, string-to-sign and key-derivation match AWS's, byte for byte.
 */
class SigV4Test {

    private static final String SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    @Test
    void reproducesTheAwsDocumentationSignature() {
        Map<String, List<String>> query = new LinkedHashMap<>();
        query.put("Action", List.of("ListUsers"));
        query.put("Version", List.of("2010-05-08"));
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("content-type", "application/x-www-form-urlencoded; charset=utf-8");
        headers.put("host", "iam.amazonaws.com");
        headers.put("x-amz-date", "20150830T123600Z");
        List<String> signedHeaders = List.of("content-type", "host", "x-amz-date");
        String emptyPayloadHash = SigV4.hex(SigV4.sha256(new byte[0]));

        String canonicalHash = SigV4.canonicalRequestHash("GET", "/", query, headers,
                signedHeaders, emptyPayloadHash);
        assertThat(canonicalHash)
                .isEqualTo("f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59");

        SigV4.Credential credential =
                SigV4.Credential.parse("AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request");
        String stringToSign = SigV4.stringToSign("20150830T123600Z", credential.scope(),
                canonicalHash);
        byte[] signingKey = SigV4.signingKey(SECRET, credential);
        assertThat(SigV4.signature(signingKey, stringToSign))
                .isEqualTo("5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7");
    }

    @Test
    void parsesTheAuthorizationHeader() {
        SigV4.AuthorizationHeader header = SigV4.AuthorizationHeader.parse(
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request, "
                        + "SignedHeaders=host;x-amz-date, Signature=abc123");
        assertThat(header.credential().accessKeyId()).isEqualTo("AKIDEXAMPLE");
        assertThat(header.credential().region()).isEqualTo("us-east-1");
        assertThat(header.credential().service()).isEqualTo("s3");
        assertThat(header.signedHeaders()).containsExactly("host", "x-amz-date");
        assertThat(header.signature()).isEqualTo("abc123");
    }

    @Test
    void rawQueryParamsKeepEncodedOctetsAndFlagParams() {
        Map<String, List<String>> params =
                SigV4.rawQueryParams("/bucket/key?acl&prefix=a%2Fb&max-keys=2");
        assertThat(params.get("acl")).containsExactly("");
        assertThat(params.get("prefix")).containsExactly("a%2Fb"); // still encoded
        assertThat(params.get("max-keys")).containsExactly("2");
    }

    @Test
    void chunkSignaturesChain() {
        SigV4.Credential credential =
                SigV4.Credential.parse("AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request");
        byte[] signingKey = SigV4.signingKey(SECRET, credential);
        String seed = "seedsignature";
        String first = SigV4.chunkSignature(signingKey, "20150830T123600Z", credential.scope(),
                seed, "hello".getBytes(StandardCharsets.UTF_8));
        String second = SigV4.chunkSignature(signingKey, "20150830T123600Z", credential.scope(),
                first, new byte[0]);
        // Deterministic and chained: re-deriving from the same inputs matches, and the second
        // chunk's signature depends on the first.
        assertThat(SigV4.chunkSignature(signingKey, "20150830T123600Z", credential.scope(), seed,
                "hello".getBytes(StandardCharsets.UTF_8))).isEqualTo(first);
        assertThat(second).isNotEqualTo(first);
    }
}
