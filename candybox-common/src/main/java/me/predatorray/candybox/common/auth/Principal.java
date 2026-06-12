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
 * An authenticated identity, shared by every Candybox surface (TCP protocol, S3 gateway, admin
 * API): a {@code type} qualifying how/why the identity exists and a {@code name} unique within the
 * type, rendered as {@code "Type:name"} (e.g. {@code User:alice}, {@code Node:3}, {@code
 * Gateway:s3-1}). The same namespace is used by the authorizer's ACLs, so an S3 access key, a SASL
 * user and an ACL entry all meet on one identity.
 *
 * @param type the principal type, e.g. {@link #TYPE_USER}
 * @param name the identity within the type
 */
public record Principal(String type, String name) {

    public static final String TYPE_USER = "User";
    public static final String TYPE_NODE = "Node";
    public static final String TYPE_GATEWAY = "Gateway";
    public static final String TYPE_ADMIN = "Admin";

    /** The unauthenticated identity (S3 {@code AllUsers}); never matches a stored credential. */
    public static final Principal ANONYMOUS = new Principal(TYPE_USER, "ANONYMOUS");

    public Principal {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("principal type is required");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("principal name is required");
        }
        if (type.indexOf(':') >= 0) {
            throw new IllegalArgumentException("principal type must not contain ':': " + type);
        }
    }

    public static Principal user(String name) {
        return new Principal(TYPE_USER, name);
    }

    /** Parses the {@code "Type:name"} form; a bare name (no colon) is a {@code User}. */
    public static Principal parse(String s) {
        int colon = s.indexOf(':');
        if (colon < 0) {
            return user(s.trim());
        }
        return new Principal(s.substring(0, colon).trim(), s.substring(colon + 1).trim());
    }

    public boolean isAnonymous() {
        return ANONYMOUS.equals(this);
    }

    @Override
    public String toString() {
        return type + ":" + name;
    }
}
