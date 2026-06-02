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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Serves the React/MUI SPA from the classpath under {@code /ui/*}. The bundle is produced by the
 * {@code candybox-web} module under the Maven {@code -Pfrontend} profile and packaged into its jar
 * at root {@code /ui/*}, so a single classloader lookup is enough.
 *
 * <p>Two important behaviors:
 *
 * <ul>
 *   <li><b>SPA fallback.</b> Any path under {@code /ui/} that doesn't resolve to a real asset (so:
 *       client-side routes like {@code /ui/boxes/photos}) falls back to {@code index.html}.
 *       Otherwise React Router only works on the root page.</li>
 *   <li><b>Missing-bundle placeholder.</b> When the {@code candybox-web} jar was built without the
 *       {@code -Pfrontend} profile, no assets exist; instead of 404'ing every request (which a
 *       browser hitting {@code /} would interpret as a broken server), we serve a small inline HTML
 *       page that tells the operator how to build the UI. Saves a confused bug report.</li>
 * </ul>
 */
final class StaticUiHandler implements HttpHandler {

    /** Classpath root that {@code candybox-web}'s frontend-maven-plugin populates. */
    static final String CLASSPATH_ROOT = "ui";

    private final Function<String, byte[]> loader;

    StaticUiHandler() {
        this(StaticUiHandler::loadResource);
    }

    /** Visible for testing — lets a fake resource map drive the handler without classpath setup. */
    StaticUiHandler(Function<String, byte[]> loader) {
        this.loader = loader;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // Strip the "/ui/" prefix; an empty resulting path means the user asked for /ui/ itself.
        String path = exchange.getRequestURI().getPath();
        String relative = path.startsWith("/ui/") ? path.substring("/ui/".length())
                : path.equals("/ui") ? "" : path;
        if (relative.contains("..")) {
            // Defense in depth — getResource won't escape the classpath, but reject the request
            // verbatim so the audit log shows the attempt.
            respond(exchange, 400, "text/plain; charset=utf-8",
                    "bad request\n".getBytes(StandardCharsets.UTF_8));
            return;
        }
        if (relative.isEmpty()) {
            relative = "index.html";
        }

        byte[] bytes = loader.apply(relative);
        String served = relative;
        if (bytes == null) {
            // Fall back to index.html for SPA client-side routes (no file extension or path under a
            // route). If the bundle isn't there at all, index.html will also be null and we'll fall
            // through to the placeholder.
            if (!looksLikeAsset(relative)) {
                bytes = loader.apply("index.html");
                served = "index.html";
            }
        }
        if (bytes == null) {
            byte[] placeholder = MISSING_BUNDLE_HTML.getBytes(StandardCharsets.UTF_8);
            respond(exchange, 200, "text/html; charset=utf-8", placeholder);
            return;
        }
        // Use the served resource's extension for Content-Type — when we SPA-fall-back to
        // index.html, the originally requested path (e.g. `boxes/photos`) has no extension and
        // would otherwise become application/octet-stream, which browsers download instead of
        // render. That's the manual-refresh "downloads the page" bug.
        respond(exchange, 200, contentTypeFor(served), bytes);
    }

    private static boolean looksLikeAsset(String path) {
        // Anything with a recognized file extension is treated as a real asset request; 404 it
        // rather than masking a typo with index.html.
        int dot = path.lastIndexOf('.');
        if (dot < 0) {
            return false;
        }
        String ext = path.substring(dot + 1).toLowerCase();
        return switch (ext) {
            case "js", "css", "map", "png", "jpg", "jpeg", "gif", "svg", "ico", "webp",
                 "woff", "woff2", "ttf", "json", "txt", "wasm" -> true;
            default -> false;
        };
    }

    private static String contentTypeFor(String path) {
        int dot = path.lastIndexOf('.');
        if (dot < 0) {
            return "application/octet-stream";
        }
        return switch (path.substring(dot + 1).toLowerCase()) {
            case "html" -> "text/html; charset=utf-8";
            case "js" -> "application/javascript; charset=utf-8";
            case "css" -> "text/css; charset=utf-8";
            case "json" -> "application/json; charset=utf-8";
            case "map" -> "application/json; charset=utf-8";
            case "svg" -> "image/svg+xml";
            case "png" -> "image/png";
            case "jpg", "jpeg" -> "image/jpeg";
            case "gif" -> "image/gif";
            case "ico" -> "image/x-icon";
            case "webp" -> "image/webp";
            case "woff" -> "font/woff";
            case "woff2" -> "font/woff2";
            case "ttf" -> "font/ttf";
            case "txt" -> "text/plain; charset=utf-8";
            case "wasm" -> "application/wasm";
            default -> "application/octet-stream";
        };
    }

    private static byte[] loadResource(String relative) {
        try (InputStream in = StaticUiHandler.class.getClassLoader()
                .getResourceAsStream(CLASSPATH_ROOT + "/" + relative)) {
            if (in == null) {
                return null;
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static void respond(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static final String MISSING_BUNDLE_HTML =
            "<!doctype html><html><head><meta charset=\"utf-8\"/>"
                    + "<title>Candybox dashboard — UI bundle missing</title>"
                    + "<style>body{font:14px/1.5 -apple-system,BlinkMacSystemFont,Segoe UI,"
                    + "sans-serif;max-width:42rem;margin:4rem auto;padding:0 1rem;color:#222}"
                    + "code{background:#f4f4f4;padding:.15rem .4rem;border-radius:3px}</style>"
                    + "</head><body>"
                    + "<h1>UI bundle not built</h1>"
                    + "<p>The admin API is running, but the React dashboard wasn't packaged into "
                    + "this build. Re-run the build with the <code>frontend</code> profile:</p>"
                    + "<pre><code>mvn -Pfrontend -DskipTests package</code></pre>"
                    + "<p>Or hit the API directly at "
                    + "<a href=\"/api/cluster\"><code>/api/cluster</code></a>.</p>"
                    + "</body></html>";
}
