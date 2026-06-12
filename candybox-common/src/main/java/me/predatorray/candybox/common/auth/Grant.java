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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * One ACL entry: a grantee and the operations granted. Grantees are either an exact principal
 * ({@code User:alice}, {@code Gateway:s3-gw}) or one of the two S3-style virtual groups:
 * {@link #ALL_USERS} (everyone, including anonymous) and {@link #AUTHENTICATED_USERS}.
 * ACL grants are additive only — there is no deny entry, matching S3 ACL semantics.
 *
 * <p>Text form (used in the ZK document and the CLI): {@code <grantee>:<OP>[+<OP>...]}, e.g.
 * {@code AllUsers:READ} or {@code User:alice:READ+WRITE}.
 */
public record Grant(String grantee, Set<Operation> operations) {

    public static final String ALL_USERS = "AllUsers";
    public static final String AUTHENTICATED_USERS = "AuthenticatedUsers";

    public Grant {
        if (grantee == null || grantee.isBlank()) {
            throw new IllegalArgumentException("grantee is required");
        }
        if (operations == null || operations.isEmpty()) {
            throw new IllegalArgumentException("a grant needs at least one operation");
        }
        operations = Set.copyOf(operations);
    }

    public static Grant of(String grantee, Operation... operations) {
        return new Grant(grantee, EnumSet.copyOf(Arrays.asList(operations)));
    }

    /** Whether this entry applies to {@code principal}. */
    public boolean matches(Principal principal) {
        if (ALL_USERS.equals(grantee)) {
            return true;
        }
        if (AUTHENTICATED_USERS.equals(grantee)) {
            return !principal.isAnonymous();
        }
        return grantee.equals(principal.toString());
    }

    public String toText() {
        return grantee + ":" + operations.stream().map(Enum::name).sorted()
                .collect(Collectors.joining("+"));
    }

    public static Grant parse(String text) {
        int lastColon = text.lastIndexOf(':');
        if (lastColon <= 0 || lastColon == text.length() - 1) {
            throw new IllegalArgumentException("Malformed grant (want <grantee>:<OP[+OP...]>) : "
                    + text);
        }
        String grantee = text.substring(0, lastColon).trim();
        EnumSet<Operation> ops = EnumSet.noneOf(Operation.class);
        for (String op : text.substring(lastColon + 1).split("\\+")) {
            ops.add(Operation.valueOf(op.trim().toUpperCase()));
        }
        return new Grant(grantee, ops);
    }
}
