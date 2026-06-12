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
 * The operations the authorizer decides on. They map S3-ACL-style permissions onto Candybox's
 * surface; an operation is checked against a {@link Resource} ({@code Cluster} or {@code Box:<name>}).
 */
public enum Operation {
    /** Read object data / metadata, list keys (Box), or list boxes (Cluster, filtered per Box). */
    READ,
    /** Put/delete/copy/rename objects, range deletes, multipart uploads (Box); create a Box (Cluster). */
    WRITE,
    /** Read the ACL document. */
    READ_ACP,
    /** Replace the ACL document. */
    WRITE_ACP,
    /** Box administration: delete the Box. The owner and super-users always have it. */
    ADMIN
}
