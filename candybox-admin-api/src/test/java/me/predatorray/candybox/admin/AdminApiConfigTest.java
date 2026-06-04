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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AdminApiConfigTest {

    @Test
    void defaultsCoverEveryField() {
        AdminApiConfig c = AdminApiConfig.defaults();
        assertThat(c.port()).isEqualTo(AdminApiConfig.DEFAULT_PORT);
        assertThat(c.bindAddress()).isEqualTo("0.0.0.0");
        assertThat(c.corsAllowOrigin()).isEqualTo("*");
        assertThat(c.uiEnabled()).isTrue();
        assertThat(c.maxUploadBytes()).isEqualTo(AdminApiConfig.DEFAULT_MAX_UPLOAD_BYTES);
    }

    @Test
    void backCompat4ArgFormPicksDefaultMaxUpload() {
        // Pre-mutation call sites + tests construct the 4-arg form; the cap silently inherits the
        // default so old code keeps compiling without surprise behaviour changes.
        AdminApiConfig c = new AdminApiConfig(0, "127.0.0.1", "*", true);
        assertThat(c.maxUploadBytes()).isEqualTo(AdminApiConfig.DEFAULT_MAX_UPLOAD_BYTES);
    }

    @Test
    void withersProduceNewInstanceWithoutMutatingSource() {
        AdminApiConfig base = AdminApiConfig.defaults();
        AdminApiConfig portChanged = base.withPort(0);
        AdminApiConfig uiOff = base.withUiEnabled(false);
        AdminApiConfig smallCap = base.withMaxUploadBytes(1024);

        assertThat(portChanged.port()).isZero();
        assertThat(portChanged.bindAddress()).isEqualTo(base.bindAddress());
        assertThat(uiOff.uiEnabled()).isFalse();
        assertThat(uiOff.port()).isEqualTo(base.port());
        assertThat(smallCap.maxUploadBytes()).isEqualTo(1024);
        // The source object is unchanged — record withers are pure.
        assertThat(base.port()).isEqualTo(AdminApiConfig.DEFAULT_PORT);
        assertThat(base.uiEnabled()).isTrue();
        assertThat(base.maxUploadBytes()).isEqualTo(AdminApiConfig.DEFAULT_MAX_UPLOAD_BYTES);
    }

    @Test
    void rejectsPortOutOfRange() {
        // The compact constructor is the only place these guards live; exercise both ends so a
        // careless future "port < 0" → "port <= 0" refactor (port 0 is a valid OS-pick sentinel)
        // gets caught by this test rather than at runtime.
        assertThatThrownBy(() -> new AdminApiConfig(-1, "0.0.0.0", "*", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port out of range");
        assertThatThrownBy(() -> new AdminApiConfig(65536, "0.0.0.0", "*", true))
                .isInstanceOf(IllegalArgumentException.class);
        // Port 0 is allowed — it means "let the OS pick", which is what tests rely on.
        assertThat(new AdminApiConfig(0, "0.0.0.0", "*", true).port()).isZero();
    }

    @Test
    void rejectsBlankBindAddress() {
        assertThatThrownBy(() -> new AdminApiConfig(0, null, "*", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bindAddress");
        assertThatThrownBy(() -> new AdminApiConfig(0, "   ", "*", true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullCorsButAllowsEmptyToDisable() {
        assertThatThrownBy(() -> new AdminApiConfig(0, "0.0.0.0", null, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("corsAllowOrigin");
        // Empty string is the documented "disable CORS headers" signal — must not throw.
        assertThat(new AdminApiConfig(0, "0.0.0.0", "", true).corsAllowOrigin()).isEmpty();
    }

    @Test
    void rejectsNonPositiveMaxUpload() {
        assertThatThrownBy(() -> new AdminApiConfig(0, "0.0.0.0", "*", true, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxUploadBytes");
        assertThatThrownBy(() -> new AdminApiConfig(0, "0.0.0.0", "*", true, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
