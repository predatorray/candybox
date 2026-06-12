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
package me.predatorray.candybox.common.auth;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A Box's ACL document: the owning principal plus additive {@link Grant}s, S3-ACL style. The owner
 * implicitly holds every {@link Operation}; everyone else holds the union of the grants matching
 * them. Stored under the coordination service at {@code acls/<box>} in a line-oriented text form:
 *
 * <pre>
 *   owner=User:alice
 *   grant=AllUsers:READ
 *   grant=User:bob:READ+WRITE
 * </pre>
 */
public record BoxAcl(Principal owner, List<Grant> grants) {

    public BoxAcl {
        if (owner == null) {
            throw new IllegalArgumentException("owner is required");
        }
        grants = List.copyOf(grants);
    }

    /** A private ACL: the owner only, no grants. */
    public static BoxAcl privateTo(Principal owner) {
        return new BoxAcl(owner, List.of());
    }

    /** Whether {@code principal} may perform {@code operation} under this document. */
    public boolean permits(Principal principal, Operation operation) {
        if (owner.equals(principal)) {
            return true;
        }
        for (Grant grant : grants) {
            if (grant.operations().contains(operation) && grant.matches(principal)) {
                return true;
            }
        }
        return false;
    }

    public byte[] toBytes() {
        StringBuilder sb = new StringBuilder("owner=").append(owner).append('\n');
        for (Grant grant : grants) {
            sb.append("grant=").append(grant.toText()).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static BoxAcl fromBytes(byte[] bytes) {
        Principal owner = null;
        List<Grant> grants = new ArrayList<>();
        for (String line : new String(bytes, StandardCharsets.UTF_8).split("\n")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            if (trimmed.startsWith("owner=")) {
                owner = Principal.parse(trimmed.substring("owner=".length()));
            } else if (trimmed.startsWith("grant=")) {
                grants.add(Grant.parse(trimmed.substring("grant=".length())));
            } else {
                throw new IllegalArgumentException("Malformed ACL line: " + trimmed);
            }
        }
        if (owner == null) {
            throw new IllegalArgumentException("ACL document has no owner line");
        }
        return new BoxAcl(owner, grants);
    }
}
