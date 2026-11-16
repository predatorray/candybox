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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.tls.PemTls;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class S3GatewayServerSecurityTest {
    private static Properties properties() {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "unused:2181");
        props.setProperty("s3.bind", "127.0.0.1:0");
        props.setProperty("s3.worker-threads", "2");
        return props;
    }

    @Test
    void s3AuthRequiresCredentialsFile() {
        Properties props = properties();
        props.setProperty("s3.auth.enabled", "true");
        S3GatewayConfig config = S3GatewayConfig.fromProperties(props, Map.of());
        assertThatThrownBy(() -> new S3GatewayServer(config, new FakeCandyStore()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("auth.credentials.file");
    }

    @Test
    void credentialsFileAuthenticatesSignedSocketRequests(@TempDir Path tmp) throws Exception {
        Path credentials = tmp.resolve("credentials.properties");
        Files.writeString(credentials, "s3.key.AKID.secret=secret\ns3.key.AKID.principal=User:alice\n");
        Properties props = properties();
        props.setProperty("s3.auth.enabled", "true");
        props.setProperty("s3.auth.allow-anonymous", "false");
        props.setProperty("auth.credentials.file", credentials.toString());
        FakeCandyStore store = new FakeCandyStore();
        try (S3GatewayServer server = new S3GatewayServer(S3GatewayConfig.fromProperties(props, Map.of()), store)) {
            server.start();
            URI uri = URI.create("http://127.0.0.1:" + server.port() + "/photos");
            HttpClient http = HttpClient.newHttpClient();
            HttpResponse<String> unsigned = http.send(HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(5)).PUT(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(unsigned.statusCode()).isEqualTo(403);
            assertThat(store.getBoxAcl("photos")).isEmpty();
            HttpResponse<String> signed = http.send(signedPut(uri, "secret"), HttpResponse.BodyHandlers.ofString());
            assertThat(signed.statusCode()).isEqualTo(200);
            assertThat(store.getBoxAcl("photos").orElseThrow().owner()).isEqualTo(Principal.user("alice"));
            HttpResponse<String> forged = http.send(signedPut(uri, "wrong"), HttpResponse.BodyHandlers.ofString());
            assertThat(forged.statusCode()).isEqualTo(403);
            assertThat(forged.body()).contains("SignatureDoesNotMatch");
        }
    }

    @Test
    void tlsListenerServesHttpsAndRejectsPlainHttp() throws Exception {
        Properties props = properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", resource("server.pem").toString());
        props.setProperty("tls.key.path", resource("server.key").toString());
        try (S3GatewayServer server = new S3GatewayServer(
                S3GatewayConfig.fromProperties(props, Map.of()), new FakeCandyStore())) {
            server.start();
            HttpClient http = HttpClient.newBuilder()
                    .sslContext(PemTls.clientContext(resource("ca.pem"), null, null)).build();
            HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                    URI.create("https://localhost:" + server.port() + "/"))
                    .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains("ListAllMyBucketsResult");
            assertThatThrownBy(() -> HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + server.port() + "/"))
                    .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString()))
                    .isInstanceOf(IOException.class);
        }
    }

    private static Path resource(String name) throws Exception {
        return Path.of(S3GatewayServerSecurityTest.class.getResource("/tls/" + name).toURI());
    }

    private static HttpRequest signedPut(URI uri, String secret) {
        String amzDate = SigV4.AMZ_DATE.format(Instant.now());
        String hash = SigV4.hex(SigV4.sha256(new byte[0]));
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("host", uri.getRawAuthority());
        headers.put("x-amz-content-sha256", hash);
        headers.put("x-amz-date", amzDate);
        List<String> names = List.copyOf(headers.keySet());
        SigV4.Credential credential = new SigV4.Credential("AKID", amzDate.substring(0, 8), "us-east-1", "s3");
        String canonical = SigV4.canonicalRequestHash("PUT", uri.getRawPath(), Map.of(), headers, names, hash);
        String signature = SigV4.signature(SigV4.signingKey(secret, credential),
                SigV4.stringToSign(amzDate, credential.scope(), canonical));
        return HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5))
                .header("x-amz-date", amzDate).header("x-amz-content-sha256", hash)
                .header("Authorization", SigV4.ALGORITHM + " Credential=AKID/" + credential.scope()
                        + ", SignedHeaders=" + String.join(";", names) + ", Signature=" + signature)
                .PUT(HttpRequest.BodyPublishers.noBody()).build();
    }
}
