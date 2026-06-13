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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class PemTlsTest {

    private static Path resource(String name) throws URISyntaxException {
        return Path.of(PemTlsTest.class.getResource("/tls/" + name).toURI());
    }

    @Test
    void readsCertificatesAndPkcs8Key() throws Exception {
        assertEquals(1, PemTls.readCertificates(resource("server.pem")).size());
        assertEquals("RSA", PemTls.readPrivateKey(resource("server.key")).getAlgorithm());
    }

    @Test
    void buildsServerAndClientContexts() throws Exception {
        assertNotNull(PemTls.serverContext(resource("server.pem"), resource("server.key"),
                resource("ca.pem")));
        assertNotNull(PemTls.clientContext(resource("ca.pem"), null, null));
    }

    @Test
    void legacyPkcs1KeyGetsAConversionHint() throws Exception {
        Path pkcs1 = resource("legacy-pkcs1.key");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PemTls.readPrivateKey(pkcs1));
        assertTrue(e.getMessage().contains("openssl pkcs8 -topk8"));
    }

    @Test
    void missingBlocksAreReported() throws Exception {
        Path certOnly = resource("server.pem");
        assertThrows(IllegalArgumentException.class, () -> PemTls.readPrivateKey(certOnly));
        Path keyOnly = resource("server.key");
        assertThrows(IllegalArgumentException.class, () -> PemTls.readCertificates(keyOnly));
    }

    @Test
    void encryptedKeysAreRejectedWithAClearMessage() throws Exception {
        Path encrypted = resource("encrypted.key");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PemTls.readPrivateKey(encrypted));
        assertTrue(e.getMessage().contains("unencrypted PKCS#8"));
    }

    @Test
    void certWithoutAKeyAndUnreadablePathsAreRejected() throws Exception {
        Path cert = resource("server.pem");
        assertThrows(IllegalArgumentException.class, () -> PemTls.serverContext(cert, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> PemTls.readCertificates(Path.of("/no/such/file.pem")));
        assertThrows(IllegalArgumentException.class,
                () -> PemTls.readPrivateKey(Path.of("/no/such/key.pem")));
    }
}
