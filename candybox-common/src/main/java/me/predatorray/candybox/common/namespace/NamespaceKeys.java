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
package me.predatorray.candybox.common.namespace;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import me.predatorray.candybox.common.exception.ValidationException;

/** Collision-free physical keys. All namespace operations route by lifecycle(address). */
public final class NamespaceKeys {
    private NamespaceKeys() { }
    private static String hex(String value) {
        if (value == null || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw new ValidationException("Namespace strings must be valid Unicode");
        }
        return HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8));
    }
    public static String address(String address) {
        String encoded = hex(address);
        if (encoded.isEmpty() || encoded.length() > 256) {
            throw new ValidationException("Namespace address must be 1..128 UTF-8 bytes");
        }
        return encoded;
    }
    public static String lifecycle(String address) { return "l/" + address(address); }
    public static String prefix(String address) { return "i/" + address(address) + "/"; }
    public static String item(String address, String key) { return prefix(address) + hex(key); }
    public static String decode(String hex) {
        return new String(HexFormat.of().parseHex(hex), StandardCharsets.UTF_8);
    }
}
