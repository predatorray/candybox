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
package me.predatorray.candybox.common.tls;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PemTlsMisconfigurationTest {
    @TempDir
    Path directory;

    @Test
    void emptyFileReportsMissingPemBlocks() throws Exception {
        Path file = Files.writeString(directory.resolve("empty.pem"), "");
        assertThatThrownBy(() -> PemTls.readCertificates(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No PEM blocks found in " + file);
    }

    @Test
    void invalidCertificateDerHasOperatorFacingError() throws Exception {
        Path file = pem("CERTIFICATE");
        assertThatThrownBy(() -> PemTls.readCertificates(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid certificate in " + file);
    }

    @Test
    void invalidPkcs8DerReportsUnsupportedKey() throws Exception {
        Path file = pem("PRIVATE KEY");
        assertThatThrownBy(() -> PemTls.readPrivateKey(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported private key algorithm in " + file);
    }

    private Path pem(String type) throws Exception {
        return Files.writeString(directory.resolve("invalid.pem"),
                "-----BEGIN " + type + "-----\nAQIDBA==\n-----END " + type + "-----\n");
    }
}
