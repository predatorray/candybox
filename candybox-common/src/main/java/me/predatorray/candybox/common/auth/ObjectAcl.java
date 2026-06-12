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

import java.util.List;

/**
 * One object's owner + grants, carried inside its {@code CandyLocator} (format v3). S3 semantics:
 * object grants are unioned with the Box ACL for READ / READ_ACP / WRITE_ACP decisions — writes and
 * deletes are governed by the Box's WRITE alone. {@code owner} may be null for objects written
 * before authorization existed (or with auth disabled): such objects follow the Box ACL only.
 *
 * @param owner  the owning principal's {@code Type:name} form, or null when unowned
 * @param grants additive grants, S3-ACL style (no deny)
 */
public record ObjectAcl(String owner, List<Grant> grants) {

    public static final ObjectAcl NONE = new ObjectAcl(null, List.of());

    public ObjectAcl {
        grants = grants == null ? List.of() : List.copyOf(grants);
    }

    public static ObjectAcl ownedBy(Principal owner) {
        return new ObjectAcl(owner == null ? null : owner.toString(), List.of());
    }

    /** Whether the object's own document permits {@code operation} (the Box ACL is checked first). */
    public boolean permits(Principal principal, Operation operation) {
        if (owner != null && owner.equals(principal.toString())) {
            return true;
        }
        for (Grant grant : grants) {
            if (grant.operations().contains(operation) && grant.matches(principal)) {
                return true;
            }
        }
        return false;
    }
}
