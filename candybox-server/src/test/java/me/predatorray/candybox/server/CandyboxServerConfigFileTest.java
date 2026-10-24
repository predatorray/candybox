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
package me.predatorray.candybox.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CandyboxServerConfigFileTest {

    private static final Map<String, String> CONF_DIR = Map.of("CANDYBOX_CONF_DIR", "/etc/cb");

    @Test
    void explicitArgumentWins() {
        assertThat(CandyboxServer.resolveConfigFile(new String[] {"/x/y.properties"}, CONF_DIR))
                .isEqualTo(Path.of("/x/y.properties"));
    }

    @Test
    void blankArgumentFallsBackToConfDir() {
        assertThat(CandyboxServer.resolveConfigFile(new String[] {" "}, CONF_DIR))
                .isEqualTo(Path.of("/etc/cb", "candybox.properties"));
    }

    @Test
    void blankOrMissingConfDirFallsBackToRelativeDefault() {
        Path expected = Path.of("conf", "candybox.properties");
        assertThat(CandyboxServer.resolveConfigFile(new String[0], Map.of())).isEqualTo(expected);
        assertThat(CandyboxServer.resolveConfigFile(new String[0], Map.of("CANDYBOX_CONF_DIR", " ")))
                .isEqualTo(expected);
    }
}
