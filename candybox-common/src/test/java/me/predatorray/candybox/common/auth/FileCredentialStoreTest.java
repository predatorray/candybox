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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.nio.file.attribute.FileTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileCredentialStoreTest {

    @TempDir
    Path dir;

    private Path write(String content) throws Exception {
        Path file = dir.resolve("credentials.properties");
        Files.writeString(file, content);
        return file;
    }

    @Test
    void parsesAllEntryKinds() throws Exception {
        ScramCredential scram = ScramCredential.fromPassword("pw");
        Path file = write("""
                sasl.user.alice = plain:wonderland
                sasl.scram-sha-256.alice = %s
                sasl.principal.s3-gw = Gateway:s3-gw
                sasl.user.s3-gw = plain:gw-pw
                s3.key.AKIAEXAMPLE.secret = ignored-by-this-store
                """.formatted(scram.toFileString()));
        FileCredentialStore store = new FileCredentialStore(file);
        assertEquals("plain:wonderland", store.plainVerifier("alice").orElseThrow());
        assertEquals(scram.iterations(),
                store.scramCredential("alice").orElseThrow().iterations());
        assertEquals(Principal.user("alice"), store.principalOf("alice"));
        assertEquals(new Principal(Principal.TYPE_GATEWAY, "s3-gw"), store.principalOf("s3-gw"));
        assertTrue(store.plainVerifier("mallory").isEmpty());
        assertTrue(store.scramCredential("mallory").isEmpty());
    }

    @Test
    void reloadsWhenTheFileChanges() throws Exception {
        Path file = write("sasl.user.alice = plain:one\n");
        FileCredentialStore store = new FileCredentialStore(file);
        assertEquals("plain:one", store.plainVerifier("alice").orElseThrow());

        Files.writeString(file, "sasl.user.alice = plain:two\n");
        // Force both the mtime to differ and the recheck window to elapse.
        Files.setLastModifiedTime(file, FileTime.from(Instant.now().plusSeconds(2)));
        Thread.sleep(1100);
        assertEquals("plain:two", store.plainVerifier("alice").orElseThrow());
    }

    @Test
    void keepsTheLastGoodSnapshotIfTheFileTurnsBad() throws Exception {
        Path file = write("sasl.user.alice = plain:one\n");
        FileCredentialStore store = new FileCredentialStore(file);

        Files.writeString(file, "sasl.scram-sha-256.alice = not-a-credential\n");
        Files.setLastModifiedTime(file, FileTime.from(Instant.now().plusSeconds(2)));
        Thread.sleep(1100);
        assertEquals("plain:one", store.plainVerifier("alice").orElseThrow());
    }
}
