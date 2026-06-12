# Candybox Authentication & Authorization — Implementation Plan

Status: **design locked, not yet implemented** · Last updated: 2026-06-12

This document is the concrete implementation plan for **authentication, authorization, and
transport encryption** across every Candybox channel. It follows the SASL-with-pluggable-mechanisms
model of Apache Kafka / BookKeeper / ZooKeeper, adds in-process TLS to every listener, and brings
the S3 gateway from anonymous-only to full **SigV4 + S3 ACL** semantics (bucket *and* object
level). Because Candybox is pre-release, **no backward compatibility is required**: defaults flip
to secure-by-default, and no migration tooling is built.

Read `DESIGN.md` for the storage architecture and `S3_GATEWAY_PLAN.md` for the gateway. This plan
changes the wire protocol (new opcodes, TLS), the `CandyLocator` format (v3: owner + object ACL),
the server/gateway/admin configs, and adds two SPIs (`AuthenticationProvider`, `Authorizer`).

---

## 1. Scope — channels covered

| # | Channel | Mechanism | Where the work is |
|---|---|---|---|
| 1 | Clients ↔ Candybox nodes | SASL handshake on the framed TCP protocol + TLS | protocol, server, client |
| 2 | S3 gateway ↔ Candybox nodes | Same as #1 — the gateway is a TCP client with a `Gateway:` principal | gateway config only |
| 3 | Candybox nodes ↔ nodes | **No such channel exists today** (routing is client-side; handover goes through ZK+BK fencing). When phase-3 cross-node compaction adds one, it reuses #1 with a `Node:` principal. Reserved now, no code. | — |
| 4 | Candybox ↔ BookKeeper | BK's native pluggable auth + TLS, enabled via **config passthrough** (one code change) + JAAS | bookkeeper module, dist |
| 5 | Candybox ↔ ZooKeeper | ZK SASL or digest auth + client TLS + **znode ACLs** on everything we create | coordination module |
| 6 | S3 clients ↔ S3 gateway | **SigV4** (headers + presigned + chunked/trailer payloads) + S3 ACLs + HTTPS | gateway |
| 7 | Browser/operator ↔ Admin API & dashboard | Static bearer token + HTTPS, CORS tightened | admin-api |
| 8 | Scrapers ↔ node/gateway `/metrics` | Optional shared bearer token + HTTPS; `/healthz`/`/readyz` stay open for probes | server, gateway, admin-api |
| 9 | Bookie ↔ bookie / bookie ↔ ZK / auto-recovery | Not Candybox code — JAAS/TLS configs shipped in compose/k8s examples + `OPERATIONS.md` | dist, docs |

## 2. Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| Auth framework | **SASL** with a pluggable `AuthenticationProvider` SPI; built-in **PLAIN** and **SCRAM-SHA-256** | Kafka/BK/ZK operator familiarity; JDK ships neither server impl, so both are written in-tree (~200 lines each, Kafka-style) |
| Default mechanism | **PLAIN over TLS** | TLS protects the wire, so the credential file can hold **bcrypt hashes** (server verifies the cleartext TLS delivered) — operationally simpler than SCRAM verifiers. SCRAM remains for TLS-offloaded setups |
| TLS | **In-process, every listener** (TCP protocol, S3 gateway, admin API, health/metrics), PEM-based | The "TLS at the LB" assumption is dropped. PEM file paths (not JKS) so cert-manager-mounted k8s Secrets work natively |
| TCP TLS impl | `SSLSocket`/`SSLServerSocket` wrapping the existing blocking transport | Drop-in; no Netty migration of `candybox-protocol` |
| mTLS | Optional on the TCP listener (`tls.client.auth=none\|required`) | Gives node↔node / gateway↔node auth via SASL EXTERNAL later; not required for v1 auth |
| Handshake | Kafka KIP-43/KIP-152 shape: `SASL_HANDSHAKE` + `SASL_AUTHENTICATE` opcodes, opaque tokens | Fits the framed protocol with zero codec changes; frame version stays 1 |
| Principal model | Single namespace: `User:`, `Node:`, `Gateway:`, `Admin:`, plus virtual groups `AllUsers`, `AuthenticatedUsers` | One ACL store governs the TCP API and the S3 surface |
| Authorization | `Authorizer` SPI; resources `Cluster` and `Box:<name>`; operations READ / WRITE / ADMIN / READ_ACP / WRITE_ACP | Kafka-style; checked in `NodeRequestHandler` (TCP) and `S3Handler` (S3) |
| Box ACL storage | ZooKeeper under `/candybox/acls/<box>`, cached on the gateway/nodes | Survives node failover with the rest of coordination state; `super.users` bypass for `Node:`/`Gateway:`/`Admin:` principals |
| Object ACLs | **In scope (v1)** — stored in `CandyLocator` v3 (owner principal + grant list) | Locators are opaque LWW values: compaction needs **no changes**; the GET path already resolves the locator before serving |
| Credential store | **File-based only** (k8s-Secret-native), watched for changes; SPI behind it | One mounted Secret holds SASL users (bcrypt/SCRAM) and S3 keypairs; rotation without restart |
| S3 auth | **SigV4 only** (no SigV2): header auth, presigned URLs, `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | What modern SDKs actually send (incl. 2025 default-integrity chunked+trailer bodies); SigV2 is legacy |
| Anonymous S3 | **S3-faithful + kill switch**: unsigned → `AllUsers` principal → ACLs decide (default deny); `s3.auth.allow-anonymous=false` hard-blocks all unsigned requests | Real S3 semantics (public-read works); default-deny keeps it secure; the flag is AWS "Block Public Access" in one knob |
| Gateway trust model | Gateway authenticates the S3 user and authorizes **itself**; talks to nodes as `Gateway:` super-principal. PUT/copy messages carry the end-user principal **only to stamp object ownership** | Kafka-REST-proxy model; no impersonation machinery. Nodes trust owner fields only from super-principals |
| Compatibility/migration | **None.** Auth + TLS required by default in shipped configs; compose generates dev certs + credentials; no `allow.everyone` flag, no ZK-ACL migrator, no staged rollout | Pre-release; secure-by-default is the point |
| Admin API auth | Static bearer token (`admin.auth.token`) on `/api/*`; CORS no longer `*` when auth is on | SSO/OIDC deferred (reverse proxy can provide it) |

## 3. New SPIs

```
candybox-common (or a new candybox-auth module):
  AuthenticationProviderFactory   mechanism name -> SaslServer/SaslClient factories (javax.security.sasl)
  CredentialStore                 lookup SASL verifier by user; lookup (secret, principal) by S3 access key
  Authorizer                      authorize(principal, operation, resource) -> ALLOW | DENY
```

Built-ins: `PlainAuthenticationProvider` (bcrypt-verifying server), `ScramSha256AuthenticationProvider`,
`FileCredentialStore` (single properties/JSON file, mtime-watched), `ZkAclAuthorizer` (+ in-memory fake
for unit tests, matching the repo's fake-per-SPI convention). GSSAPI/OAUTHBEARER plug in later via the
SPI; JAAS (`java.security.auth.login.config`) is honored for BK/ZK interop but is not required for
Candybox's own listeners.

The credential file (one k8s Secret):

```properties
# SASL users (PLAIN verifies against bcrypt; SCRAM against stored verifier)
sasl.user.alice = bcrypt:$2a$12$...
sasl.user.node-1 = bcrypt:$2a$12$...        # -> principal Node:1 via principal mapping below
sasl.principal.node-1 = Node:1
# S3 access keys -> retrievable secret + principal (SigV4 is HMAC over the actual secret;
# it cannot be one-way hashed — documented, file perms 0400)
s3.key.AKIAEXAMPLE.secret = wJalr...
s3.key.AKIAEXAMPLE.principal = User:alice
```

## 4. TCP protocol changes (clients/gateway/nodes ↔ nodes)

New opcodes (frame format unchanged, payloads opaque to `FrameCodec`):

```
SASL_HANDSHAKE(50)        req: mechanism name        resp: OK | enabled-mechanism list on mismatch
SASL_AUTHENTICATE(51)     req: opaque byte[] token   resp: opaque byte[] challenge + complete flag
RESPONSE_AUTH_FAILED(52)  terminal auth error — clients must not retry (unlike BUSY/MOVED)
```

Connection lifecycle: TLS handshake (always, unless `tls.enabled=false` for tests) →
`SASL_HANDSHAKE` → one or more `SASL_AUTHENTICATE` round trips → normal opcodes. The server gains a
per-connection `ConnectionContext { state, principal }` in `TcpTransportServer`'s per-socket loop
(today there is zero per-connection state); `RequestHandler.handle(Frame)` becomes
`handle(ConnectionContext, Frame)`. Any non-SASL opcode on an unauthenticated connection →
`RESPONSE_AUTH_FAILED`. Re-authentication of long-lived connections (Kafka KIP-368) is deferred.

`NodeRequestHandler` authorizes after decode: box ops map to READ (get/head/list/range-get) /
WRITE (put/delete/delete-range/copy/rename/multipart) / ADMIN (create/delete box) on `Box:<name>`;
`LIST_BOXES`/`CREATE_BOX` check `Cluster`. List results are filtered to readable boxes.

## 5. Object ownership & ACLs — `CandyLocator` v3

`CandyLocatorSerializer` bumps to **version 3** (no compatibility shim needed pre-release):

```
v2 fields ...
string  ownerPrincipal                  (e.g. "User:alice"; stamped at PUT/copy/complete-multipart)
varint  grant count [+ {string grantee, byte permission}]   permission: READ, READ_ACP, WRITE_ACP, FULL_CONTROL
```

- **Compaction/GC: untouched.** Locators are opaque LWW values; grants ride along.
- **GET/HEAD object**: allowed if the *bucket* ACL grants READ **or** the object grants READ to the
  principal (S3 union-of-grants; ACLs have no deny). The locator is already resolved on this path.
- **PUT/DELETE/overwrite**: governed by bucket WRITE (S3 semantics — object ACLs do not gate writes).
- **`PUT ?acl` on an object**: metadata-only locator rewrite reusing the parts verbatim (the
  zero-copy `copy` trick onto the same key), new HLC, fenced like any write.
- **CopyObject**: destination does **not** inherit the source ACL; owner = requesting principal,
  ACL = canned ACL from the request (default `private`). Rename keeps owner+ACL.
- Canned ACLs map to grants: `public-read` → `AllUsers:READ`; `public-read-write` adds bucket-level
  `AllUsers:WRITE`; `authenticated-read` → `AuthenticatedUsers:READ`; `bucket-owner-*` resolved at
  grant time. `GET ?acl` renders the stored owner + grants as the standard XML document.
- New opcodes `GET_CANDY_ACL` / `SET_CANDY_ACL`; `PUT_CANDY`/`COPY_CANDY`/`CREATE_MULTIPART_UPLOAD`
  messages gain `ownerPrincipal` + canned-ACL fields (accepted only from super-principals).

## 6. S3 gateway: SigV4 + S3 semantics

Verification order: requests **with** auth material that fails → terminal 403
(`SignatureDoesNotMatch` / `InvalidAccessKeyId` / `RequestTimeTooSkewed`, ±15 min) — never falls
back to anonymous. Requests **without** auth material → principal `AllUsers` (unless
`s3.auth.allow-anonymous=false`, which short-circuits 403 `AccessDenied`).

Must-support payload modes (`x-amz-content-sha256`):
1. literal SHA-256 (verified), 2. `UNSIGNED-PAYLOAD`,
3. `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` — aws-chunked with per-chunk signatures (AWS CLI/SDK default
   for plain-HTTP PUT); the existing aws-chunked decoder grows signature verification,
4. `STREAMING-UNSIGNED-PAYLOAD-TRAILER` — chunked with `x-amz-checksum-*` trailers, the **default**
   for SDKs since the 2025 integrity rollout (boto3 ≥ 1.36). Non-optional.

Plus: presigned URLs (query auth, `X-Amz-Expires` ≤ 7 days, payload UNSIGNED), configurable region
(default `us-east-1`) / service `s3` scope validation, `SignatureDoesNotMatch` errors include the
server-side `StringToSign` for debuggability, and a documented requirement that any proxy in front
passes `Host` through unchanged (the signature covers it). HTTPS via Netty `SslHandler` (PEM).

New S3 endpoints: `GET/PUT Bucket ?acl`, `GET/PUT Object ?acl`, canned `x-amz-acl` on
PUT/Copy/CreateBucket/CreateMultipartUpload. `Owner` in listings comes from the locator's owner.
Expected allowlist growth: the 13 currently-vacuous anonymous/raw/ACL tests pass genuinely, plus the
`test_bucket_acl_canned*`, `test_object_acl*`, presigned, and multi-account isolation families
(`s3 main` vs `s3 alt` finally mean different principals). Recalibrate and commit the new baseline.

## 7. BookKeeper & ZooKeeper (external configs)

- **BK passthrough** (the one code change in `candybox-bookkeeper`): every `bookkeeper.client.<key>`
  property / `CANDYBOX_BOOKKEEPER_CLIENT_*` env var is copied verbatim into `ClientConfiguration` in
  `BookKeeperLedgerStore.create()`. That enables `clientAuthProviderFactoryClass` (BK SASL),
  client↔bookie TLS, and any future BK setting with no further releases. Bookie-side
  (`bookieAuthProviderFactoryClass`, TLS, JAAS) ships in compose/k8s examples + `OPERATIONS.md`.
- **ZK**: `zookeeper.auth.scheme/credentials` wired to Curator `.authorization(...)` (digest) or
  JAAS `Client` section (SASL — JVM-global, conveniently shared with BK's internal ZK client);
  client TLS via `client.secure` + ssl props. An `ACLProvider` on the Curator builder stamps
  `sasl:<principal>:cdrwa` / `auth::cdrwa` on every znode under the `candybox` namespace
  (`zookeeper.acl.enabled`, default true). No migration tool — pre-release.

## 8. Admin API, health & metrics

`admin.auth.token` bearer guard on `/api/*` and dashboard data calls; SPA login = paste token.
CORS allow-origin becomes configurable (no `*` with auth on). Node/gateway `/metrics` accept an
optional shared bearer token that the admin scraper presents; `/healthz`/`/readyz` stay
unauthenticated for probes. All three HTTP servers gain HTTPS (JDK `HttpsServer` / Netty).

## 9. Config summary (all env-overridable, `CANDYBOX_` prefix)

```properties
auth.enabled = true                      # false only for tests/dev
auth.sasl.mechanisms = PLAIN,SCRAM-SHA-256
auth.credentials.file = /etc/candybox/credentials.properties
auth.super.users = Gateway:s3,Admin:ops,Node:*
tls.enabled = true
tls.cert.path / tls.key.path / tls.truststore.path     # PEM
tls.client.auth = none | required                       # TCP listener mTLS
zookeeper.auth.scheme / zookeeper.auth.credentials / zookeeper.acl.enabled
bookkeeper.client.*                                     # verbatim passthrough
s3.auth.enabled = true
s3.auth.allow-anonymous = true                          # the kill switch
s3.auth.region = us-east-1
admin.auth.token / metrics.auth.token
```

## 10. Phasing

| Phase | Deliverable |
|---|---|
| A | TLS on the TCP transport + SASL handshake (opcodes, `ConnectionContext`, PLAIN + SCRAM, SPIs, `FileCredentialStore`); client + CLI support; contract tests with fakes |
| B | `Authorizer` SPI + Box/Cluster checks in `NodeRequestHandler`, ZK-backed Box ACLs, super-users, `candybox acl` CLI; **locator v3** (owner + object grants) + `GET/SET_CANDY_ACL` opcodes |
| C | BK `ClientConfiguration` passthrough + ZK auth/ACLProvider/TLS; compose & k8s examples with secured ZK/bookies; `OPERATIONS.md` security chapter |
| D | Gateway: HTTPS, SigV4 (all four payload modes + presigned), anonymous-as-`AllUsers` + kill switch, bucket/object ACL endpoints, canned ACLs; **recalibrate ceph/s3-tests allowlist** |
| E | Admin API bearer auth + HTTPS, metrics tokens, dashboard login |

A ∥ C are independent; B needs A's principals; D needs A + B. Each phase lands with its tests
(fakes for unit, embedded BK/ZK + real TLS for ITs) and keeps `mvn verify` green.

## 11. Deferred (explicitly out of scope)

SASL re-authentication (KIP-368) and connection max-age; OAUTHBEARER/GSSAPI built-ins (SPI-ready);
bucket *policies* (JSON policy language — ACLs only for now); ZK-backed credential store; per-request
impersonation beyond owner stamping; SSO/OIDC on the admin API; audit logging (cheap to add at the
two authorize() call sites later); SigV2.
