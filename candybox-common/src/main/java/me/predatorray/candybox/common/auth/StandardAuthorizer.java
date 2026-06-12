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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The standard {@link Authorizer}: super-users first, then the Box's ACL document. The ACL source
 * is a function so the node (coordination-backed store) and the gateway (cached client lookups)
 * plug in their own; both evaluate identically.
 *
 * <p>Cluster-level policy (fixed in v1): any <em>authenticated</em> principal may create a Box
 * (becoming its owner) and list Boxes (results are filtered per-Box by READ); anonymous may not.
 * A Box with no ACL document (created before authorization existed) falls back to
 * <em>authenticated-full-access</em> rather than locking everyone out.
 */
public final class StandardAuthorizer implements Authorizer {

    private final Set<String> superUsers;
    private final Function<String, Optional<BoxAcl>> aclSource;

    /**
     * @param superUsers principals that bypass every check ({@code Node:*}-style trailing-wildcard
     *                   entries match a whole principal type)
     * @param aclSource  resolves a Box name to its ACL document, empty if none exists
     */
    public StandardAuthorizer(List<String> superUsers,
                              Function<String, Optional<BoxAcl>> aclSource) {
        this.superUsers = superUsers.stream().map(String::trim).collect(Collectors.toSet());
        this.aclSource = aclSource;
    }

    @Override
    public boolean authorize(Principal principal, Operation operation, Resource resource) {
        if (isSuperUser(principal)) {
            return true;
        }
        if (resource.type() == Resource.Type.CLUSTER) {
            return !principal.isAnonymous();
        }
        Optional<BoxAcl> acl = aclSource.apply(resource.name());
        if (acl.isEmpty()) {
            return !principal.isAnonymous();
        }
        return acl.get().permits(principal, operation);
    }

    @Override
    public boolean isSuperUser(Principal principal) {
        return superUsers.contains(principal.toString())
                || superUsers.contains(principal.type() + ":*");
    }
}
