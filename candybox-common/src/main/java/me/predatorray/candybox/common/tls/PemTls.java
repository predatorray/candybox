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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Builds {@link SSLContext}s from PEM files — certificate chain, PKCS#8 private key, and an
 * optional CA bundle — so Kubernetes cert-manager Secrets (and plain {@code openssl} output) mount
 * directly with no JKS conversion. Used by every Candybox listener (TCP protocol, S3 gateway,
 * admin/health HTTP) and by clients to trust a private CA.
 *
 * <p>Keys must be unencrypted PKCS#8 ({@code -----BEGIN PRIVATE KEY-----}); the legacy PKCS#1 /
 * SEC1 forms are rejected with a conversion hint ({@code openssl pkcs8 -topk8 -nocrypt}).
 */
public final class PemTls {

    private static final Pattern PEM_BLOCK =
            Pattern.compile("-----BEGIN ([A-Z0-9 ]+)-----([^-]+)-----END \\1-----");
    private static final char[] EMPTY_PASSWORD = new char[0];

    private PemTls() {
    }

    /** A server-side context presenting {@code certChain}/{@code key}; {@code caBundle} (nullable)
     * is the trust used to verify client certificates when mTLS is required. */
    public static SSLContext serverContext(Path certChain, Path key, Path caBundle) {
        return context(certChain, key, caBundle);
    }

    /** A client-side context trusting {@code caBundle} (null ⇒ JVM default trust); the optional
     * {@code certChain}/{@code key} (nullable) is the client certificate for mTLS. */
    public static SSLContext clientContext(Path caBundle, Path certChain, Path key) {
        return context(certChain, key, caBundle);
    }

    private static SSLContext context(Path certChain, Path key, Path caBundle) {
        try {
            KeyManagerFactory kmf = null;
            if (certChain != null) {
                if (key == null) {
                    throw new IllegalArgumentException("tls key path is required with a cert path");
                }
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                keyStore.load(null, null);
                List<X509Certificate> chain = readCertificates(certChain);
                keyStore.setKeyEntry("key", readPrivateKey(key), EMPTY_PASSWORD,
                        chain.toArray(new Certificate[0]));
                kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, EMPTY_PASSWORD);
            }

            TrustManagerFactory tmf = null;
            if (caBundle != null) {
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                trustStore.load(null, null);
                int i = 0;
                for (X509Certificate ca : readCertificates(caBundle)) {
                    trustStore.setCertificateEntry("ca-" + i++, ca);
                }
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);
            }

            SSLContext context = SSLContext.getInstance("TLS");
            context.init(kmf == null ? null : kmf.getKeyManagers(),
                    tmf == null ? null : tmf.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalArgumentException("Failed to build TLS context", e);
        }
    }

    /** Reads every certificate in a PEM file (a chain or CA bundle), in file order. */
    public static List<X509Certificate> readCertificates(Path pemFile) {
        List<X509Certificate> certs = new ArrayList<>();
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            for (PemBlock block : readBlocks(pemFile)) {
                if (block.type().equals("CERTIFICATE")) {
                    certs.add((X509Certificate) factory.generateCertificate(
                            new java.io.ByteArrayInputStream(block.der())));
                }
            }
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("Invalid certificate in " + pemFile, e);
        }
        if (certs.isEmpty()) {
            throw new IllegalArgumentException("No CERTIFICATE block found in " + pemFile);
        }
        return certs;
    }

    /** Reads an unencrypted PKCS#8 private key (RSA, EC or Ed25519). */
    public static PrivateKey readPrivateKey(Path pemFile) {
        for (PemBlock block : readBlocks(pemFile)) {
            switch (block.type()) {
                case "PRIVATE KEY" -> {
                    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(block.der());
                    for (String algorithm : new String[] {"RSA", "EC", "Ed25519"}) {
                        try {
                            return KeyFactory.getInstance(algorithm).generatePrivate(spec);
                        } catch (InvalidKeySpecException tryNext) {
                            // not this algorithm
                        } catch (GeneralSecurityException e) {
                            throw new IllegalArgumentException(
                                    "Failed to read private key from " + pemFile, e);
                        }
                    }
                    throw new IllegalArgumentException(
                            "Unsupported private key algorithm in " + pemFile);
                }
                case "RSA PRIVATE KEY", "EC PRIVATE KEY" -> throw new IllegalArgumentException(
                        pemFile + " holds a legacy " + block.type() + " (PKCS#1/SEC1); convert it "
                                + "with: openssl pkcs8 -topk8 -nocrypt -in <key> -out <key>.pk8");
                case "ENCRYPTED PRIVATE KEY" -> throw new IllegalArgumentException(
                        pemFile + " is password-protected; provide an unencrypted PKCS#8 key");
                default -> {
                    // skip non-key blocks (e.g. a certificate in the same file)
                }
            }
        }
        throw new IllegalArgumentException("No PRIVATE KEY block found in " + pemFile);
    }

    private record PemBlock(String type, byte[] der) {
    }

    private static List<PemBlock> readBlocks(Path pemFile) {
        String content;
        try {
            content = Files.readString(pemFile, StandardCharsets.US_ASCII);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read PEM file " + pemFile, e);
        }
        List<PemBlock> blocks = new ArrayList<>();
        Matcher m = PEM_BLOCK.matcher(content);
        while (m.find()) {
            byte[] der = Base64.getMimeDecoder().decode(m.group(2));
            blocks.add(new PemBlock(m.group(1), der));
        }
        if (blocks.isEmpty()) {
            throw new IllegalArgumentException("No PEM blocks found in " + pemFile);
        }
        return blocks;
    }
}
