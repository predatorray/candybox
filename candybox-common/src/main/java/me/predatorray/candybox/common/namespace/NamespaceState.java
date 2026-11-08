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

import java.util.Map;

/** Durable lifecycle stored as a payload-free live locator, never a GC-able tombstone. */
public record NamespaceState(long version, long createdAtMillis, long idleTtlMillis,
                             long expiresAtMillis, boolean retired) {
    public NamespaceState {
        if (version < 1 || createdAtMillis < 0 || idleTtlMillis < 1
                || idleTtlMillis > 365L * 24 * 60 * 60 * 1000 || expiresAtMillis < createdAtMillis) {
            throw new IllegalArgumentException("Invalid namespace lifecycle");
        }
    }

    public Map<String, String> metadata() {
        return Map.of("format", "1", "version", Long.toString(version),
                "created", Long.toString(createdAtMillis), "ttl", Long.toString(idleTtlMillis),
                "expires", Long.toString(expiresAtMillis), "retired", Boolean.toString(retired));
    }
    public static NamespaceState from(Map<String, String> m) {
        if (!"1".equals(m.get("format"))) {
            throw new IllegalStateException("Unsupported namespace lifecycle format");
        }
        if (!"true".equals(m.get("retired")) && !"false".equals(m.get("retired"))) {
            throw new IllegalStateException("Invalid namespace retirement state");
        }
        return new NamespaceState(Long.parseLong(m.get("version")),
                Long.parseLong(m.get("created")), Long.parseLong(m.get("ttl")),
                Long.parseLong(m.get("expires")), Boolean.parseBoolean(m.get("retired")));
    }
}
