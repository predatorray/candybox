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

/**
 * The authorization SPI: decides whether a principal may perform an operation on a resource. The
 * standard implementation is {@link StandardAuthorizer} (super-users + Box ACLs); the S3 gateway
 * runs the same checks against the same documents, so one ACL governs both front doors.
 */
@FunctionalInterface
public interface Authorizer {

    /** Permits everything — the behavior when authentication/authorization is disabled. */
    Authorizer ALLOW_ALL = (principal, operation, resource) -> true;

    boolean authorize(Principal principal, Operation operation, Resource resource);
}
