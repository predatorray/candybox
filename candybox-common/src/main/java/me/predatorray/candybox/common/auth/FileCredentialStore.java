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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The file-backed {@link CredentialStore}: one properties file (typically a mounted Kubernetes
 * Secret) holding every kind of credential. Reloaded automatically when the file's modification
 * time changes (checked at most once a second), so credential rotation needs no restart.
 *
 * <pre>
 *   # SASL PLAIN verifiers — produced by `candybox make-credentials` (PBKDF2; `plain:` works for dev)
 *   sasl.user.alice = pbkdf2-sha256:120000:&lt;saltB64&gt;:&lt;hashB64&gt;
 *   # SASL SCRAM-SHA-256 credentials
 *   sasl.scram-sha-256.alice = salt=&lt;b64&gt;,iterations=4096,storedKey=&lt;b64&gt;,serverKey=&lt;b64&gt;
 *   # Optional principal mapping (default User:&lt;username&gt;)
 *   sasl.principal.s3-gw = Gateway:s3-gw
 *   # S3 access keys (SigV4 needs the actual secret — protect this file)
 *   s3.key.AKIAEXAMPLE.secret = wJalr...
 *   s3.key.AKIAEXAMPLE.principal = User:alice
 * </pre>
 */
public final class FileCredentialStore implements CredentialStore, S3KeyStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileCredentialStore.class);

    private static final String PLAIN_PREFIX = "sasl.user.";
    private static final String SCRAM_PREFIX = "sasl.scram-sha-256.";
    private static final String PRINCIPAL_PREFIX = "sasl.principal.";
    private static final String S3_KEY_PREFIX = "s3.key.";
    private static final long RECHECK_INTERVAL_MILLIS = 1000;

    private final Path file;
    private volatile Snapshot snapshot;
    private volatile long nextRecheckAtMillis;

    public FileCredentialStore(Path file) {
        this.file = file;
        this.snapshot = Snapshot.load(file);
        this.nextRecheckAtMillis = System.currentTimeMillis() + RECHECK_INTERVAL_MILLIS;
    }

    @Override
    public Optional<String> plainVerifier(String username) {
        return Optional.ofNullable(current().plainVerifiers.get(username));
    }

    @Override
    public Optional<ScramCredential> scramCredential(String username) {
        return Optional.ofNullable(current().scramCredentials.get(username));
    }

    @Override
    public Principal principalOf(String username) {
        return current().principals.getOrDefault(username, Principal.user(username));
    }

    @Override
    public Optional<S3Key> s3Key(String accessKeyId) {
        return Optional.ofNullable(current().s3Keys.get(accessKeyId));
    }

    private Snapshot current() {
        long now = System.currentTimeMillis();
        if (now >= nextRecheckAtMillis) {
            synchronized (this) {
                if (now >= nextRecheckAtMillis) {
                    nextRecheckAtMillis = now + RECHECK_INTERVAL_MILLIS;
                    reloadIfChanged();
                }
            }
        }
        return snapshot;
    }

    private void reloadIfChanged() {
        try {
            Instant mtime = Files.getLastModifiedTime(file).toInstant();
            if (!mtime.equals(snapshot.mtime)) {
                snapshot = Snapshot.load(file);
                LOG.info("Reloaded credentials from {} ({} PLAIN user(s), {} SCRAM user(s))", file,
                        snapshot.plainVerifiers.size(), snapshot.scramCredentials.size());
            }
        } catch (IOException | RuntimeException e) {
            // Keep serving the last good snapshot; a half-written rotation must not lock everyone out.
            LOG.warn("Failed to reload credentials from {}; keeping previous set: {}", file,
                    e.toString());
        }
    }

    private record Snapshot(Instant mtime, Map<String, String> plainVerifiers,
                            Map<String, ScramCredential> scramCredentials,
                            Map<String, Principal> principals, Map<String, S3Key> s3Keys) {

        static Snapshot load(Path file) {
            Properties props = new Properties();
            Instant mtime;
            try (InputStream in = Files.newInputStream(file)) {
                mtime = Files.getLastModifiedTime(file).toInstant();
                props.load(in);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read credentials file: " + file, e);
            }
            Map<String, String> plain = new LinkedHashMap<>();
            Map<String, ScramCredential> scram = new LinkedHashMap<>();
            Map<String, Principal> principals = new LinkedHashMap<>();
            Map<String, String> s3Secrets = new LinkedHashMap<>();
            Map<String, Principal> s3Principals = new LinkedHashMap<>();
            for (String key : props.stringPropertyNames()) {
                String value = props.getProperty(key).trim();
                if (key.startsWith(PLAIN_PREFIX)) {
                    plain.put(key.substring(PLAIN_PREFIX.length()), value);
                } else if (key.startsWith(SCRAM_PREFIX)) {
                    scram.put(key.substring(SCRAM_PREFIX.length()), ScramCredential.parse(value));
                } else if (key.startsWith(PRINCIPAL_PREFIX)) {
                    principals.put(key.substring(PRINCIPAL_PREFIX.length()),
                            Principal.parse(value));
                } else if (key.startsWith(S3_KEY_PREFIX)) {
                    String rest = key.substring(S3_KEY_PREFIX.length());
                    if (rest.endsWith(".secret")) {
                        s3Secrets.put(rest.substring(0, rest.length() - ".secret".length()), value);
                    } else if (rest.endsWith(".principal")) {
                        s3Principals.put(rest.substring(0, rest.length() - ".principal".length()),
                                Principal.parse(value));
                    }
                }
                // Unknown prefixes are not an error: one file serves every credential kind.
            }
            Map<String, S3Key> s3Keys = new LinkedHashMap<>();
            for (Map.Entry<String, String> e : s3Secrets.entrySet()) {
                Principal principal = s3Principals.getOrDefault(e.getKey(),
                        Principal.user(e.getKey()));
                s3Keys.put(e.getKey(), new S3Key(e.getKey(), e.getValue(), principal));
            }
            return new Snapshot(mtime, Map.copyOf(plain), Map.copyOf(scram),
                    Map.copyOf(principals), Map.copyOf(s3Keys));
        }
    }
}
