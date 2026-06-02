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
}
