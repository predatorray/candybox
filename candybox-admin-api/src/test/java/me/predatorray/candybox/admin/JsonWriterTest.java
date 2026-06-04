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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonWriterTest {

    @Test
    void writesPrimitives() {
        assertThat(JsonWriter.write(null)).isEqualTo("null");
        assertThat(JsonWriter.write(true)).isEqualTo("true");
        assertThat(JsonWriter.write(42)).isEqualTo("42");
        assertThat(JsonWriter.write(3.5)).isEqualTo("3.5");
        assertThat(JsonWriter.write("hi")).isEqualTo("\"hi\"");
    }

    @Test
    void escapesControlCharsAndQuotes() {
        assertThat(JsonWriter.write("a\"b\\c\nd\te")).isEqualTo("\"a\\\"b\\\\c\\nd\\te\"");
        // bell (0x07) is below the printable range and must be u00xx-style escaped.
        String bell = String.valueOf((char) 0x07);
        assertThat(JsonWriter.write(bell)).isEqualTo("\"\\u0007\"");
    }

    @Test
    void writesObjectInInsertionOrder() {
        // Use LinkedHashMap so the assertion can pin field order — predictable output is what makes
        // this writer easy to eyeball in curl.
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("a", 1);
        obj.put("b", List.of("x", "y"));
        obj.put("c", null);
        assertThat(JsonWriter.write(obj)).isEqualTo("{\"a\":1,\"b\":[\"x\",\"y\"],\"c\":null}");
    }

    @Test
    void rejectsUnsupportedTypes() {
        assertThatThrownBy(() -> JsonWriter.write(new Object()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonWriter.write(Map.of(1, "v")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonWriter.write(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInfinitiesAndNullKey() {
        // The writeNumber guards cover both Double and Float; pin both branches so a future
        // refactor that drops one doesn't silently emit "Infinity" (which strict parsers reject).
        assertThatThrownBy(() -> JsonWriter.write(Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonWriter.write(Float.NEGATIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JsonWriter.write(Float.NaN))
                .isInstanceOf(IllegalArgumentException.class);

        // A LinkedHashMap can legally hold a null key — JsonWriter must reject it with a clear
        // message rather than NPE'ing on the cast.
        Map<Object, Object> withNullKey = new LinkedHashMap<>();
        withNullKey.put(null, "v");
        assertThatThrownBy(() -> JsonWriter.write(withNullKey))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a String");
    }

    @Test
    void writesNestedShapesAndAllNumericFlavours() {
        // Mixed Integer / Long / Float / BigInteger / BigDecimal — every Number subclass routes
        // through writeNumber and emits its toString() form. Pin these so a future "wrap numbers
        // in quotes for JS bigint safety" change is a deliberate decision, not a regression.
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("ints", List.of(1, 2L, (short) 3, (byte) 4));
        root.put("floats", List.of(1.5f, 2.5d));
        root.put("big", new java.math.BigInteger("123456789012345678901234567890"));
        root.put("decimal", new java.math.BigDecimal("0.10"));
        // List.of rejects nulls so use Arrays.asList for the case that needs to embed one.
        root.put("nested", List.of(Map.of("k", "v"),
                java.util.Arrays.asList(true, false, null)));

        String json = JsonWriter.write(root);
        assertThat(json).contains("\"ints\":[1,2,3,4]")
                .contains("\"floats\":[1.5,2.5]")
                .contains("\"big\":123456789012345678901234567890")
                .contains("\"decimal\":0.10")
                .contains("\"nested\":[{\"k\":\"v\"},[true,false,null]]");
    }

    @Test
    void escapesEveryNamedControlChar() {
        // Each named-escape branch in the switch needs to fire so the table is fully exercised.
        String body = "\b\f\r/normal";
        assertThat(JsonWriter.write(body)).isEqualTo("\"\\b\\f\\r/normal\"");
        // 0x1F is the upper bound of the unicode-escape range — anything above is a literal char.
        assertThat(JsonWriter.write(String.valueOf((char) 0x1F))).isEqualTo("\"\\u001f\"");
        // ASCII printable above 0x1F passes through unchanged.
        assertThat(JsonWriter.write("AZ09")).isEqualTo("\"AZ09\"");
    }

    @Test
    void emptyContainersSerializeCleanly() {
        // The "first" flag flips on the first entry; the no-entries branch never hits the comma
        // path. These are easy to break with an off-by-one — assert both.
        assertThat(JsonWriter.write(java.util.Map.of())).isEqualTo("{}");
        assertThat(JsonWriter.write(java.util.List.of())).isEqualTo("[]");
    }
}
