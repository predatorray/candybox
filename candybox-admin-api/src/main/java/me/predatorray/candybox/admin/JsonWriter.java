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

import java.util.List;
import java.util.Map;

/**
 * A 100-line hand-written JSON writer. The admin API ships maps/lists/numbers/strings/booleans and
 * nothing else, which is well below the threshold where adding Jackson or Gson earns its keep.
 * Keeping it dependency-free also matches how the rest of candybox handles serialization (see
 * {@code BinaryWriter}): one small focused tool, no reflection, no surprises.
 *
 * <p>The writer accepts only the JSON-native shapes — {@link Map} (string keys), {@link List},
 * {@link Number}, {@link Boolean}, {@link String}, and {@code null} — and rejects everything else
 * with {@link IllegalArgumentException} so a typo at a call site fails loudly rather than emitting
 * {@code "com.example.Foo@1234"}.
 */
public final class JsonWriter {

    private JsonWriter() {
    }

    public static String write(Object value) {
        StringBuilder out = new StringBuilder(64);
        writeValue(out, value);
        return out.toString();
    }

    private static void writeValue(StringBuilder out, Object value) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof String s) {
            writeString(out, s);
        } else if (value instanceof Boolean b) {
            out.append(b.booleanValue() ? "true" : "false");
        } else if (value instanceof Number n) {
            writeNumber(out, n);
        } else if (value instanceof Map<?, ?> m) {
            writeObject(out, m);
        } else if (value instanceof List<?> l) {
            writeArray(out, l);
        } else {
            throw new IllegalArgumentException(
                    "unsupported JSON value type: " + value.getClass().getName());
        }
    }

    private static void writeObject(StringBuilder out, Map<?, ?> map) {
        out.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> e : map.entrySet()) {
            if (!(e.getKey() instanceof String key)) {
                throw new IllegalArgumentException("JSON object key must be a String, got "
                        + (e.getKey() == null ? "null" : e.getKey().getClass().getName()));
            }
            if (!first) {
                out.append(',');
            }
            first = false;
            writeString(out, key);
            out.append(':');
            writeValue(out, e.getValue());
        }
        out.append('}');
    }

    private static void writeArray(StringBuilder out, List<?> list) {
        out.append('[');
        boolean first = true;
        for (Object item : list) {
            if (!first) {
                out.append(',');
            }
            first = false;
            writeValue(out, item);
        }
        out.append(']');
    }

    private static void writeNumber(StringBuilder out, Number n) {
        // Reject NaN/Infinity — they're not valid JSON; surfacing as a runtime error beats silently
        // writing "NaN" which a strict parser would reject anyway.
        if (n instanceof Double d && (d.isNaN() || d.isInfinite())) {
            throw new IllegalArgumentException("non-finite double cannot be JSON-encoded: " + d);
        }
        if (n instanceof Float f && (f.isNaN() || f.isInfinite())) {
            throw new IllegalArgumentException("non-finite float cannot be JSON-encoded: " + f);
        }
        out.append(n.toString());
    }

    private static void writeString(StringBuilder out, String s) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
