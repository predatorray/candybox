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
package me.predatorray.candybox.common.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CredentialFileParsingTest {
    @TempDir
    Path directory;

    @Test
    void s3AccessKeyMapsToConfiguredPrincipal() throws Exception {
        Path file = Files.writeString(directory.resolve("credentials.properties"),
                "s3.key.AKID1.secret = abc\ns3.key.AKID1.principal = user:alice\n");
        FileCredentialStore store = new FileCredentialStore(file);
        assertThat(store.s3Key("AKID1")).contains(new S3Key("AKID1", "abc", Principal.user("alice")));
        assertThat(store.s3Key("AKID2")).isEmpty();
    }

    @Test
    void unknownScramAttributeIsRejected() {
        assertThatThrownBy(() -> ScramCredential.parse("x=1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown SCRAM credential attribute: x");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "iterations=1,storedKey=AQ==,serverKey=Ag==",
            "salt=AA==,iterations=1,serverKey=Ag==",
            "salt=AA==,iterations=1,storedKey=AQ==",
            "salt=AA==,storedKey=AQ==,serverKey=Ag==",
            "salt=AA==,iterations=0,storedKey=AQ==,serverKey=Ag==",
            "salt=AA==,iterations=-1,storedKey=AQ==,serverKey=Ag=="
    })
    void incompleteOrNonpositiveIterationScramCredentialsAreRejected(String value) {
        assertThatThrownBy(() -> ScramCredential.parse(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("SCRAM credential requires salt, iterations, storedKey and serverKey");
    }

    @Test
    void aclIgnoresCommentsAndBlankLinesAndRoundTrips() {
        BoxAcl plain = BoxAcl.fromBytes("owner=User:alice\ngrant=User:bob:READ+WRITE\n"
                .getBytes(StandardCharsets.UTF_8));
        BoxAcl decorated = BoxAcl.fromBytes(("\n# owner\nowner=User:alice\n  \n"
                + "  # grant\ngrant=User:bob:READ+WRITE\n\n").getBytes(StandardCharsets.UTF_8));
        assertThat(decorated).isEqualTo(plain);
        assertThat(BoxAcl.fromBytes(decorated.toBytes())).isEqualTo(plain);
    }
}
