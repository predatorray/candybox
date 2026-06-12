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
package me.predatorray.candybox.coordination.zk;

/**
 * How a Candybox process authenticates to ZooKeeper and whether the znodes it creates are
 * ACL-protected ({@code CREATOR_ALL_ACL}).
 *
 * <ul>
 *   <li>{@code scheme="digest"}, {@code credentials="user:password"} — ZooKeeper digest auth, no
 *       server-side setup beyond knowing the digest.</li>
 *   <li>{@code scheme=null} with a JVM-global JAAS {@code Client} section — ZooKeeper SASL
 *       (DIGEST-MD5 / Kerberos); set {@code aclEnabled} to protect created znodes.</li>
 *   <li>ZooKeeper client TLS is configured through ZooKeeper's standard system properties
 *       ({@code zookeeper.client.secure}, {@code zookeeper.ssl.*}) — see {@code OPERATIONS.md}.</li>
 * </ul>
 *
 * @param scheme      the {@code addAuth} scheme ({@code digest}), or null for none/SASL-via-JAAS
 * @param credentials the scheme credentials (digest: {@code user:password}), or null
 * @param aclEnabled  stamp {@code CREATOR_ALL_ACL} on every created znode
 */
public record ZkAuth(String scheme, String credentials, boolean aclEnabled) {

    public static final ZkAuth NONE = new ZkAuth(null, null, false);
}
