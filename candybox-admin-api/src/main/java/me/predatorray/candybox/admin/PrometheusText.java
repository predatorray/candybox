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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A minimal parser for the Prometheus text exposition format — enough to extract the
 * {@code (name, labels, value)} tuples each storage node's {@code /metrics} endpoint emits, no
 * more. We deliberately do not bring in the official Prometheus Java client just to read a few
 * gauge samples on a polling thread: the format the rest of candybox emits is the trivial subset
 * (comments + bare samples), and the contract is unlikely to grow histograms here.
 *
 * <p>Handled lines:
 *
 * <ul>
 *   <li>Comments — {@code # HELP …}, {@code # TYPE …}: skipped.</li>
 *   <li>{@code name value}.</li>
 *   <li>{@code name{k1="v1",k2="v2"} value}.</li>
 *   <li>{@code name value timestamp_ms} — the trailing timestamp is ignored; the scraper assigns
 *       its own ingest timestamp so all series share one clock.</li>
 * </ul>
 *
 * <p>Skipped lines:
 *
 * <ul>
 *   <li>Any value that isn't a finite double ({@code NaN}, {@code +Inf}, {@code -Inf}).</li>
 *   <li>Lines that don't match the above shape (defensive — never throws).</li>
 * </ul>
 */
final class PrometheusText {

    private PrometheusText() {
    }

    /** One parsed sample: metric name, sorted-by-insertion labels, finite double value. */
    record Sample(String name, Map<String, String> labels, double value) {
    }

    static List<Sample> parse(String text) {
        List<Sample> out = new ArrayList<>();
        for (String line : text.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            Sample s = parseLine(trimmed);
            if (s != null) {
                out.add(s);
            }
        }
        return out;
    }

    private static Sample parseLine(String line) {
        // The name is everything up to the first whitespace, '{', or end-of-line.
        int nameEnd = 0;
        while (nameEnd < line.length()) {
            char c = line.charAt(nameEnd);
            if (c == ' ' || c == '\t' || c == '{') {
                break;
            }
            nameEnd++;
        }
        if (nameEnd == 0) {
            return null;
        }
        String name = line.substring(0, nameEnd);
        int cursor = nameEnd;

        Map<String, String> labels;
        if (cursor < line.length() && line.charAt(cursor) == '{') {
            int close = line.indexOf('}', cursor);
            if (close < 0) {
                return null;
            }
            labels = parseLabels(line.substring(cursor + 1, close));
            cursor = close + 1;
        } else {
            labels = Map.of();
        }

        // Skip any whitespace then take the value token; ignore an optional trailing timestamp.
        while (cursor < line.length() && Character.isWhitespace(line.charAt(cursor))) {
            cursor++;
        }
        if (cursor >= line.length()) {
            return null;
        }
        int valueEnd = cursor;
        while (valueEnd < line.length() && !Character.isWhitespace(line.charAt(valueEnd))) {
            valueEnd++;
        }
        String token = line.substring(cursor, valueEnd);
        double value;
        try {
            value = Double.parseDouble(token);
        } catch (NumberFormatException e) {
            return null;
        }
        if (!Double.isFinite(value)) {
            return null;
        }
        return new Sample(name, labels, value);
    }

    private static Map<String, String> parseLabels(String inside) {
        Map<String, String> out = new LinkedHashMap<>();
        int i = 0;
        while (i < inside.length()) {
            // Skip leading separators / whitespace.
            while (i < inside.length() && (inside.charAt(i) == ',' || Character.isWhitespace(inside.charAt(i)))) {
                i++;
            }
            if (i >= inside.length()) {
                break;
            }
            int eq = inside.indexOf('=', i);
            if (eq < 0) {
                break;
            }
            String key = inside.substring(i, eq).trim();
            i = eq + 1;
            if (i >= inside.length() || inside.charAt(i) != '"') {
                break;
            }
            int end = i + 1;
            StringBuilder val = new StringBuilder();
            while (end < inside.length() && inside.charAt(end) != '"') {
                char c = inside.charAt(end);
                if (c == '\\' && end + 1 < inside.length()) {
                    // Prometheus only escapes \\ and \" inside label values; treat any unknown
                    // escape as the literal character following the backslash so we don't drop data.
                    char next = inside.charAt(end + 1);
                    val.append(next == 'n' ? '\n' : next);
                    end += 2;
                } else {
                    val.append(c);
                    end += 1;
                }
            }
            if (end >= inside.length()) {
                break;
            }
            out.put(key, val.toString());
            i = end + 1;
        }
        return out;
    }
}
