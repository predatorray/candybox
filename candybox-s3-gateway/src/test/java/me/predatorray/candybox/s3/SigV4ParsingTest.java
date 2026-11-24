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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SigV4ParsingTest {
    @ParameterizedTest
    @ValueSource(strings = {"AKID/20250615/us-east-1/s3", "AKID/20250615/us-east-1/s3/other"})
    void malformedCredentialScope(String scope) {
        assertThatIllegalArgumentException().isThrownBy(() -> SigV4.Credential.parse(scope))
                .withMessageContaining("Malformed SigV4 credential scope");
    }

    @ParameterizedTest
    @ValueSource(strings = {"Credential", "SignedHeaders", "Signature"})
    void missingAuthorizationField(String field) {
        String valid = SigV4.ALGORITHM + " Credential=AKID/20250615/us-east-1/s3/aws4_request, "
                + "SignedHeaders=host, Signature=012345";
        assertThat(SigV4.AuthorizationHeader.parse(valid).signature()).isEqualTo("012345");
        String malformed = valid.replaceAll(field + "=[^,]*(, )?", "");
        assertThatIllegalArgumentException().isThrownBy(() -> SigV4.AuthorizationHeader.parse(malformed))
                .withMessage("Malformed SigV4 Authorization header");
    }

    @Test
    void emptyCanonicalUriIsSlash() {
        String slash = SigV4.canonicalRequestHash("GET", "/", Map.of(), Map.of("host", "example.com"),
                List.of("host"), SigV4.UNSIGNED_PAYLOAD);
        assertThat(SigV4.canonicalRequestHash("GET", "", Map.of(), Map.of("host", "example.com"),
                List.of("host"), SigV4.UNSIGNED_PAYLOAD)).isEqualTo(slash);
    }
}
