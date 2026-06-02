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
package me.predatorray.candybox.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class AdminApiMainTest {

    @Test
    void defaultsWhenEnvIsEmpty() {
        AdminApiConfig c = AdminApiMain.fromEnv(Map.of());
        assertThat(c.port()).isEqualTo(AdminApiConfig.DEFAULT_PORT);
        assertThat(c.bindAddress()).isEqualTo("0.0.0.0");
        assertThat(c.corsAllowOrigin()).isEqualTo("*");
        assertThat(c.uiEnabled()).isTrue();
    }

    @Test
    void honoursEnvOverrides() {
        AdminApiConfig c = AdminApiMain.fromEnv(Map.of(
                "CANDYBOX_ADMIN_PORT", "19712",
                "CANDYBOX_ADMIN_BIND", "127.0.0.1",
                "CANDYBOX_ADMIN_CORS", "https://example.com",
                "CANDYBOX_ADMIN_UI", "false"));
        assertThat(c.port()).isEqualTo(19712);
        assertThat(c.bindAddress()).isEqualTo("127.0.0.1");
        assertThat(c.corsAllowOrigin()).isEqualTo("https://example.com");
        assertThat(c.uiEnabled()).isFalse();
    }

    @Test
    void ignoresNonNumericPort() {
        AdminApiConfig c = AdminApiMain.fromEnv(Map.of("CANDYBOX_ADMIN_PORT", "not-a-port"));
        assertThat(c.port()).isEqualTo(AdminApiConfig.DEFAULT_PORT);
    }
}
