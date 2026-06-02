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

/**
 * Boot-time configuration for the admin/dashboard HTTP server. Kept as a small immutable record so
 * the composition root ({@link AdminApiMain}) can hand it to {@link AdminApiServer} once and the
 * server's wiring stays testable without touching system properties.
 *
 * @param port            TCP port to bind. Use {@code 0} to let the OS pick (handy in tests).
 * @param bindAddress     interface to bind to. {@code 0.0.0.0} listens on all interfaces.
 * @param corsAllowOrigin value of the {@code Access-Control-Allow-Origin} header. Defaults to
 *                        {@code *} since v1 has no auth; tighten in production by setting an
 *                        explicit origin.
 * @param uiEnabled       when false, {@code /ui/*} returns 404 instead of trying to serve the
 *                        bundled SPA. Lets operators run the admin API headless.
 * @param maxUploadBytes  hard cap on Candy upload size accepted by the dashboard's
 *                        {@code PUT /api/boxes/{name}/objects/{key}} route. The default 64 MiB is
 *                        well below the server's own per-Candy limit but matches the v1 dashboard's
 *                        "small ad-hoc uploads" posture; bump this knob before piping anything
 *                        large through the UI instead of straight at the S3 gateway.
 */
public record AdminApiConfig(int port, String bindAddress, String corsAllowOrigin,
                             boolean uiEnabled, long maxUploadBytes) {

    /**
     * Default port. Picked one slot above the gateway's health port (9712) so the existing
     * docker-compose layout (server TCP 9709 / server health 9710 / gateway S3 9711 / gateway
     * health 9712) keeps its arithmetic clean.
     */
    public static final int DEFAULT_PORT = 9713;

    /** Default cap for {@code PUT .../objects/{key}}: 64 MiB. */
    public static final long DEFAULT_MAX_UPLOAD_BYTES = 64L * 1024 * 1024;

    public AdminApiConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (bindAddress == null || bindAddress.isBlank()) {
            throw new IllegalArgumentException("bindAddress must be non-blank");
        }
        if (corsAllowOrigin == null) {
            throw new IllegalArgumentException("corsAllowOrigin must be non-null (use empty to disable)");
        }
        if (maxUploadBytes <= 0) {
            throw new IllegalArgumentException("maxUploadBytes must be positive: " + maxUploadBytes);
        }
    }

    /**
     * Back-compat 4-arg form: lets pre-mutation call sites (and tests) keep working without
     * threading the new {@code maxUploadBytes} knob through. Picks up
     * {@link #DEFAULT_MAX_UPLOAD_BYTES}.
     */
    public AdminApiConfig(int port, String bindAddress, String corsAllowOrigin, boolean uiEnabled) {
        this(port, bindAddress, corsAllowOrigin, uiEnabled, DEFAULT_MAX_UPLOAD_BYTES);
    }

    /** Sensible defaults for {@code docker compose up}: bind everywhere, CORS open, UI mounted. */
    public static AdminApiConfig defaults() {
        return new AdminApiConfig(DEFAULT_PORT, "0.0.0.0", "*", true, DEFAULT_MAX_UPLOAD_BYTES);
    }

    public AdminApiConfig withPort(int newPort) {
        return new AdminApiConfig(newPort, bindAddress, corsAllowOrigin, uiEnabled, maxUploadBytes);
    }

    public AdminApiConfig withUiEnabled(boolean enabled) {
        return new AdminApiConfig(port, bindAddress, corsAllowOrigin, enabled, maxUploadBytes);
    }

    public AdminApiConfig withMaxUploadBytes(long bytes) {
        return new AdminApiConfig(port, bindAddress, corsAllowOrigin, uiEnabled, bytes);
    }
}
