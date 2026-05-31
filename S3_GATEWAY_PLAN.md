# Candybox S3 Gateway — Implementation Plan

Status: **v1 implemented** (module `candybox-s3-gateway`) · Last updated: 2026-05-29

> **Implemented in v1:** the standalone Netty gateway module, anonymous path-style addressing, bucket
> CRUD + `ListBuckets`, single-shot `PutObject`/`GetObject`/`HeadObject`/`DeleteObject`, `CopyObject`
> (same-bucket), batch `DeleteObjects`, `ListObjectsV2`/V1 with delimiter rollup + continuation tokens,
> deterministic CRC32C ETag, the S3 error model, `aws-chunked` decoding, ignored auth, and the canned
> startup-probe subresources. Launch with `bin/candybox-s3-gateway`. Unit tests live in the module;
> `S3GatewayIT` drives it end-to-end over a real node. **Phase 5 additions:** Range GET (HTTP 206
> with `Content-Range`), multipart upload (`CreateMultipartUpload`/`UploadPart`/`Complete`/`Abort`
> plus `UploadPartCopy`, `ListMultipartUploads`, `ListParts`). **Still deferred** (per §16): real
> MD5 ETags, virtual-host addressing, SigV4. See [`MULTIPART_RANGE_PLAN.md`](MULTIPART_RANGE_PLAN.md)
> for the engine-level changes.

This document is the concrete implementation plan for an **S3-compatible HTTP gateway** in front of
Candybox. It is a *translation layer*: it speaks the S3 REST/XML protocol to clients and the existing
Candybox TCP wire protocol (via `CandyboxClient`) to the cluster. It introduces **no new on-ledger or
wire format** and depends on no new Candybox engine features for v1.

Read `DESIGN.md` for the storage architecture and `README.md` for the module map. This plan does not
change either; it adds one new leaf module.

---

## 1. Scope

### In scope (v1)

- A standalone, **stateless** gateway process (`candybox-s3-gateway`) built on **Netty**.
- **Anonymous** access only — no request signing/verification (SigV4 ignored, see §10).
- **Path-style** bucket addressing only: `https://s3.host/<bucket>/<key>` (see §5).
- Bucket ops: `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListBuckets`.
- Object ops: single-shot `PutObject`, `GetObject` (full object), `HeadObject`, `DeleteObject`,
  `CopyObject`, `DeleteObjects` (batch).
- `ListObjectsV2` (and V1 `marker` compatibility) with `prefix`, `delimiter`, `max-keys`,
  pagination, and synthesized `CommonPrefixes`.
- **Deterministic ETag derived from the stored CRC32C** (see §8).
- S3-shaped XML responses and error bodies (see §6, §9).
- Canned responses for the handful of subresource requests SDKs probe on startup (see §10).
- Health/metrics endpoints and `CANDYBOX_*` config parity with the node (see §11, §12).

### Out of scope (v1) — see §16 for the deferred-feature plan

- **Multipart upload** (`CreateMultipartUpload`/`UploadPart`/`Complete`/`Abort`, plus
  `UploadPartCopy`, `ListMultipartUploads`, `ListParts`) — **now implemented**. See
  [`MULTIPART_RANGE_PLAN.md`](MULTIPART_RANGE_PLAN.md). v1 limitation: each part is bounded by the
  16 MiB frame cap (until on-the-wire streaming lands; the data model is decoupled from streaming).
- **Real MD5 ETag + `Content-MD5`/checksum verification** — requires a new object-metadata layer to
  persist the MD5 alongside the object; deferred.
- **Range GET (HTTP 206)** — **now implemented**. Single range only (`bytes=A-B`/`A-`/`-N`);
  multi-range is rejected with 501 because it requires a `multipart/byteranges` response. See
  [`MULTIPART_RANGE_PLAN.md`](MULTIPART_RANGE_PLAN.md).
- **TLS** — terminated at the external HTTP(S) load balancer, which is **out of scope**. The gateway
  listens on plain HTTP behind the LB.
- Authentication/authorization (SigV4), bucket policies, ACLs, CORS config, lifecycle, object
  tagging, versioning, storage classes, multi-region.

### Assumptions

- An external HTTP(S) **load balancer** terminates TLS and fan-outs requests across gateway instances.
  Gateways are interchangeable and hold no per-request state, so the LB may use any policy (round
  robin is fine). The LB, its DNS, and its certificate are **not** part of this work.
- The gateway reaches the cluster in **cluster-routing mode** (`CandyboxClient` constructed with a
  `CoordinationService`), so it resolves each Box's fenced owner via ZooKeeper and follows `MOVED`
  exactly like the CLI does in a cluster.

---

## 2. Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| HTTP server | **Netty** | Async I/O, streaming bodies, backpressure; avoids buffering whole objects. |
| TLS | At the **load balancer** (out of scope) | Gateway stays plain-HTTP and simple; LB owns certs. |
| Auth | **Anonymous** (ignore `Authorization`) | Trusted-network deployment; SigV4 deferred. |
| Addressing | **Path-style only** | One hostname/cert at the LB; no wildcard DNS. |
| ETag | **Deterministic from CRC32C** | Stable across PUT/GET/HEAD, zero extra storage; real MD5 deferred. |
| Range GET | **HTTP 206 with `Content-Range`** | Single range only (`bytes=A-B`/`A-`/`-N`); multi-range rejected as 501. |
| Multipart | **Implemented** | `Create`/`UploadPart`/`Complete`/`Abort` + `UploadPartCopy` + `ListMultipartUploads`/`ListParts`. ETag is CRC32C-hex with `-N` suffix for multipart objects. |
| Object size | PUT and per-part bounded by the frame cap (16 MiB until streaming lands) | Multipart enables larger objects by parts; each part still buffered. |

---

## 3. Module & dependencies

New Maven module **`candybox-s3-gateway`**, added to the root `pom.xml` `<modules>` list (after
`candybox-client`).

**Depends on:**
- `candybox-client` — the `CandyboxClient` API and its `CandyInfo` / `Listing` records.
- `candybox-common` — `BoxName`, `CandyKey`, `CandyboxConfig`, exception types, CRC32C helper.
- `candybox-coordination` — `CoordinationService` for cluster routing (same as the CLI's cluster mode).
- `candybox-protocol` — `TcpTransport` (transitively via the client).
- **Netty** (`io.netty:netty-all` or the codec-http + transport artifacts) — new third-party dep.
  Pin the version in the root `pom.xml` `<properties>` (`netty.version`) like the other deps.

**Must NOT depend on** `candybox-lsm`, `candybox-bookkeeper`, or the raw BookKeeper/ZooKeeper clients
— the gateway is a *client* of the cluster, not a storage node. This respects the module dependency
rule in `CLAUDE.md`.

Suggested package: `me.predatorray.candybox.s3`.

```
candybox-s3-gateway/
  src/main/java/me/predatorray/candybox/s3/
    S3GatewayMain.java          # entrypoint: load config, build CandyboxClient, start Netty
    S3GatewayConfig.java        # CANDYBOX_*/properties config (mirrors ServerConfig style)
    S3GatewayServer.java        # Netty bootstrap, channel pipeline, graceful shutdown
    http/
      S3RequestRouter.java      # parse method+path+query -> S3Operation
      S3Operation.java          # enum/sealed: PutObject, GetObject, ListObjectsV2, ...
      S3RequestHandler.java     # Netty inbound handler; dispatches to ops on a worker pool
      BlockingBridge.java       # Netty <-> InputStream/OutputStream bridges (see §4)
    op/                         # one class per operation, each calls CandyboxClient
      BucketOps.java            # create/delete/head/list buckets
      ObjectOps.java            # put/get/head/delete/copy
      ListObjectsOp.java        # listing + delimiter rollup + pagination
      DeleteObjectsOp.java      # batch delete
      CannedSubresourceOps.java # ?location, ?versioning, ?acl probes (§10)
    xml/
      S3XmlWriter.java          # streaming XML for list results, copy result, errors
      S3Error.java              # error code catalog + HTTP status
    ErrorMapper.java            # CandyboxException -> S3Error (§9)
    Etag.java                   # crc32c -> ETag string (§8)
  src/test/java/...             # unit tests (fakes / in-JVM)
```

Integration tests live in **`candybox-integration-tests`** (the `*IT` convention), not in this module
— see §13.

---

## 4. Architecture & request flow

### Threading model (the important part)

`CandyboxClient`/`TcpTransport` are **blocking**. Netty's event-loop threads must never block. So the
gateway uses the standard Netty pattern: the event loop handles HTTP framing and backpressure, and all
`CandyboxClient` calls run on a **dedicated blocking worker pool** (`DefaultEventExecutorGroup` or a
plain `ExecutorService` handed the request). Bridges connect the two worlds:

- **PutObject (request body → Candybox):** Netty delivers `HttpContent` chunks on the event loop. They
  are fed into a bounded blocking buffer that the worker thread reads as an `InputStream`, passed
  straight to `client.putCandy(box, key, inputStream, contentType, userMetadata)`. Backpressure: when
  the buffer is full, toggle channel auto-read off so the client/LB slows down. (`BlockingBridge`.)
- **GetObject (Candybox → response body):** the worker thread calls
  `client.getCandy(box, key, outputStream)` where `outputStream` writes into Netty, respecting
  `Channel.isWritable()` (flush + wait when the outbound buffer fills). This streams large objects
  without buffering them in memory.

> v1 interim simplification (optional): because multipart is deferred and single PUTs are size-bounded
> (§11), an initial cut MAY use `HttpObjectAggregator` with `maxContentLength = maxObjectSize` to buffer
> the request body in memory and skip the PUT bridge. This is simpler but caps memory per concurrent
> upload. The streaming bridge above is the target design and the reason we chose Netty; prefer it for
> GET from day one (read objects can be large) and adopt it for PUT as soon as the bridge lands.

### Netty pipeline

```
SocketChannel
  └─ HttpServerCodec               (request line/headers/response encoding)
  └─ HttpContentDecompressor       (handle Content-Encoding: gzip if a client sends it)
  └─ ChunkedWriteHandler           (stream large GET responses)
  └─ S3RequestHandler              (route + dispatch to worker pool; NO blocking here)
```

Note: do **not** put a giant `HttpObjectAggregator` in the streaming design. Handle the
`aws-chunked` transfer encoding explicitly (see §10) — strip chunk framing before writing object
bytes to Candybox.

### Request lifecycle

1. Event loop parses request line + headers → `S3RequestRouter` produces an `S3Operation` from
   `(method, path, query, headers)`.
2. Handler submits the op to the worker pool with a handle to the channel and (for PUT) the body bridge.
3. Worker calls the matching `CandyboxClient` method, maps the result/exception, and writes the HTTP
   response (status + S3 headers + XML/stream body) back through the channel.
4. On `CandyboxException`, `ErrorMapper` produces the S3 status + XML error body (§9).

---

## 5. Routing & addressing (path-style)

URL shape: `/<bucket>/<key...>` (key may contain slashes). `Host` header is ignored for bucket
resolution.

| Path | Method | Operation |
|---|---|---|
| `/` | `GET` | `ListBuckets` |
| `/<bucket>` (or `/<bucket>/`) | `PUT` | `CreateBucket` |
| `/<bucket>` | `DELETE` | `DeleteBucket` |
| `/<bucket>` | `HEAD` | `HeadBucket` |
| `/<bucket>` | `GET` | `ListObjectsV2` (or V1 if `list-type` absent) |
| `/<bucket>?delete` | `POST` | `DeleteObjects` (batch) |
| `/<bucket>?location` `?versioning` `?acl` … | `GET` | canned subresource (§10) |
| `/<bucket>/<key>` | `PUT` | `PutObject`, or `CopyObject` if `x-amz-copy-source` header present |
| `/<bucket>/<key>` | `GET` | `GetObject` (full object; `Range` ignored in v1) |
| `/<bucket>/<key>` | `HEAD` | `HeadObject` |
| `/<bucket>/<key>` | `DELETE` | `DeleteObject` |

**Key handling:** URL-decode the key path segment(s). Preserve slashes within the key. Validate via
`CandyKey` (non-empty UTF-8, ≤ configured max bytes) and `BoxName` (already enforces S3 bucket rules:
3–63 chars, `[a-z0-9-]`). Validation failures → `400 InvalidBucketName` / `InvalidArgument`.

---

## 6. API surface — mapping to `CandyboxClient`

| S3 operation | CandyboxClient call | Response notes |
|---|---|---|
| `CreateBucket` | `createBox(bucket)` | `200`, `Location: /<bucket>` header. |
| `DeleteBucket` | `deleteBox(bucket, force=false)` | `204`. `BucketNotEmpty` → 409 (§9). |
| `HeadBucket` | `headBox(bucket)` | `200` if present, `404` otherwise (no body on HEAD). |
| `ListBuckets` | `listBoxes()` | `200`, `ListAllMyBucketsResult` XML; synthesize a `CreationDate` (epoch/now — Candybox has no bucket-create timestamp). |
| `PutObject` | `putCandy(box, key, InputStream, contentType, userMetadata)` | `200`, `ETag` header (§8). Map `Content-Type` and `x-amz-meta-*` → `userMetadata`. |
| `CopyObject` | `copyCandy(box, srcKey, dstKey, idempotencyToken)` | Same-bucket only (Candybox `copyCandy` is intra-Box). `x-amz-copy-source` parsed; cross-bucket copy → `501`/`NotImplemented` in v1. Response: `CopyObjectResult` XML with `ETag`. |
| `GetObject` | `getCandy(box, key, OutputStream)` | `200`, stream body; headers: `Content-Length`, `Content-Type`, `ETag`, `Last-Modified` (from `createdAtMillis`), `x-amz-meta-*`. |
| `HeadObject` | `headCandy(box, key)` → `CandyInfo` | Same headers as GET, no body. |
| `DeleteObject` | `deleteCandy(box, key)` | `204` (idempotent: deleting a missing key still returns `204`, per S3). |
| `DeleteObjects` | loop `deleteCandy` per key (optionally `deleteRange` for prefix-shaped requests) | `200`, `DeleteResult` XML listing `Deleted`/`Error` per key. |
| `ListObjectsV2` | `listCandies(prefix, startAfter, maxKeys)` | See §7. |

`CandyInfo` fields available: `contentLength`, `contentType`, `userMetadata`, `crc32c`,
`createdAtMillis`. `Listing.Entry`: `key`, `contentLength`, `createdAtMillis`.

---

## 7. Listing semantics

Map S3 `ListObjectsV2` params onto `listCandies(prefix, startAfter, maxKeys)`:

- `prefix` → `prefix`.
- `continuation-token` (V2) / `marker` (V1) / `start-after` → `startAfter`. Use the opaque
  `Listing.nextStartAfter` as the `continuation-token` we return; no need to base64 unless we want to
  hide the key (recommended: base64-encode it so it reads as opaque to clients).
- `max-keys` → `maxKeys` (clamp to 1000 default like S3).
- `Listing.isTruncated()` / `nextStartAfter` → `<IsTruncated>` + `<NextContinuationToken>`.

**`delimiter` (CommonPrefixes) is synthesized in the gateway:** when a delimiter is supplied, after
fetching a page, group keys by their first delimiter occurrence *after the prefix*; collapse each group
into a `<CommonPrefixes><Prefix>` entry and suppress the individual keys. Because rollup happens
post-fetch, a page may yield fewer than `max-keys` `Contents` entries — that is S3-legal. (A future
engine-level delimiter scan would make this cheaper; see §16.)

XML: `ListBucketResult` with `Name`, `Prefix`, `Delimiter`, `MaxKeys`, `KeyCount`, `IsTruncated`,
`Contents[]` (`Key`, `LastModified` from `createdAtMillis`, `ETag` from crc32c, `Size`,
`StorageClass=STANDARD`), `CommonPrefixes[]`.

---

## 8. ETag strategy (deterministic from CRC32C)

S3 clients expect an `ETag` header on PUT/GET/HEAD and within list entries. v1 derives it from the
CRC32C that Candybox already stores in `CandyMetadata`/`CandyInfo`:

```
ETag = quote( leftPad( hex(crc32c & 0xffffffffL), 32, '0') )
e.g. crc32c=0x8f3a2b1c  ->  ETag: "000000008f3a2b1c"
```

Properties: deterministic and stable across PUT/GET/HEAD/List for the same bytes; needs no extra
storage. **Caveat:** it is *not* the MD5 S3 normally returns, so a client that independently computes
MD5 and strictly compares may warn or reject. This is an accepted v1 limitation; real MD5 ETags are a
deferred feature (§16) because they need a new metadata layer to persist the digest.

`Etag.java` centralizes the formatting so the future swap to real MD5 touches one place.

---

## 9. Error model

Every error is an HTTP status + an S3 `<Error>` XML body:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Resource>/photos/cat.jpg</Resource>
  <RequestId>...</RequestId>
</Error>
```

`ErrorMapper` translates Candybox exceptions (all from `candybox-common`/client) to S3 codes:

| Candybox exception | HTTP | S3 `<Code>` |
|---|---|---|
| `CandyNotFoundException` | 404 | `NoSuchKey` |
| `BoxNotFoundException` | 404 | `NoSuchBucket` |
| `BoxAlreadyExistsException` | 409 | `BucketAlreadyOwnedByYou` |
| `BoxNotEmptyException` | 409 | `BucketNotEmpty` |
| `ValidationException` (bad bucket name) | 400 | `InvalidBucketName` |
| `ValidationException` (other) | 400 | `InvalidArgument` |
| `LimitExceededException` (key too long) | 400 | `KeyTooLongError` |
| `LimitExceededException` (object too large) | 400 | `EntityTooLarge` |
| `BusyException` | 503 | `SlowDown` |
| `NotOwnerException` / `FencedException` | 503 | `ServiceUnavailable` (retryable; ownership in flux) |
| `StorageException` / `SerializationException` | 500 | `InternalError` |
| anything unmapped | 500 | `InternalError` |

Notes:
- `DeleteObject` on a missing key is **not** an error in S3 — return `204` regardless.
- On `503` responses, set a `Retry-After` header so well-behaved clients back off; the LB and SDK retry
  logic will then re-drive to a (possibly new) owner.
- Generate a per-request `RequestId` (UUID) and echo it in `x-amz-request-id` and error bodies for
  debuggability.

---

## 10. Anonymous mode & SDK-compatibility shims

Even with no auth, real SDKs/CLIs behave as if talking to AWS, so the gateway must tolerate their
quirks:

- **`Authorization` / `x-amz-content-sha256` headers:** accept and **ignore**. Do not 400 on their
  presence; do not verify.
- **`aws-chunked` transfer encoding:** boto3/aws-cli may send `Content-Encoding: aws-chunked` with
  `x-amz-decoded-content-length` even unsigned. The gateway **must de-chunk** the body (strip the
  per-chunk size/signature framing) before writing object bytes — otherwise stored objects are
  corrupted. Use `x-amz-decoded-content-length` as the true `Content-Length` for the `putCandy` call.
- **`Expect: 100-continue`:** honor it (Netty can) so large PUTs don't send the body before headers
  are accepted.
- **Startup subresource probes:** return benign canned XML so clients don't choke —
  `GET /<bucket>?location` → `<LocationConstraint>` (empty / configured region),
  `GET /<bucket>?versioning` → empty `<VersioningConfiguration>`, `?acl` → a canned private ACL,
  `?cors`/`?lifecycle`/`?tagging` → `404 NoSuch*Configuration`. Keep this list in
  `CannedSubresourceOps`.
- **Client guidance (docs):** since multipart is deferred, large uploads from default SDK config will
  try multipart and fail — document raising `s3.multipart_threshold` (boto3) or
  `multipart_threshold` (aws-cli) above the max object size, and using `--endpoint-url` with
  path-style addressing (`addressing_style = path`).

---

## 11. Configuration

Follow the node's `ServerConfig` pattern exactly: `conf/*.properties` keys overridable by
`CANDYBOX_<KEY>` env vars (dots → underscores, upper-cased), env wins.

| Key | Meaning | Default |
|---|---|---|
| `s3.bind` | Address the gateway listens on (plain HTTP, behind the LB). | `0.0.0.0:9711` |
| `zookeeper.connect` | ZooKeeper connect string for cluster routing (shared with the cluster). | `127.0.0.1:2181` |
| `s3.region` | Value returned in `?location` and any region echoes. | `us-east-1` |
| `s3.max-object-bytes` | Reject single PUTs larger than this (no multipart). | from `SizeLimits` / e.g. 5 GiB |
| `s3.worker-threads` | Size of the blocking worker pool calling `CandyboxClient`. | `2 × cores` |
| `s3.router-cache-ttl-ms` | Box→owner resolution cache TTL (passed to the cluster router). | client default |
| `health.port` | HTTP port for `/healthz`, `/readyz`, `/metrics`. | `9712` |

The gateway builds `CandyboxClient(transport, coordinationService, candyboxConfig)` (cluster mode), so
it needs `zookeeper.connect` rather than a single `host:port`.

---

## 12. Health, metrics, operations

- Reuse the node's health-server shape: `/healthz` (liveness — process up), `/readyz` (readiness —
  ZooKeeper reachable + at least one route resolvable), `/metrics`. Run it on `health.port`,
  separate from the S3 listener, so the LB can health-check independently.
- Metrics to expose: request count/latency per operation, error counts by S3 code, in-flight uploads,
  bytes in/out, worker-pool saturation, `MOVED` re-routes.
- **Graceful shutdown:** stop accepting new connections, drain in-flight requests (bounded), then close
  Netty groups and the `CandyboxClient`. The LB removes the instance on `/readyz` failure.
- Packaging: add a gateway launcher to `candybox-dist` (`bin/candybox-s3-gateway`) and a third mode to
  the Docker image (alongside node + CLI), plus a Compose service and a K8s `Deployment` (stateless —
  not a `StatefulSet`) under `examples/`.

---

## 13. Testing strategy

Mirror the repo's split (unit fakes vs `*IT` on real backends; no mocking frameworks):

**Unit tests (`candybox-s3-gateway/src/test`, `mvn test`):**
- `S3RequestRouter` path/query → operation mapping (incl. `?delete`, subresources, copy-source header).
- `ErrorMapper` exhaustive exception → status/code table (§9).
- `Etag` formatting.
- `S3XmlWriter` golden-XML tests for `ListBucketResult`, `ListAllMyBucketsResult`, `DeleteResult`,
  `CopyObjectResult`, `Error`.
- Listing delimiter rollup / `CommonPrefixes` logic over a fake `Listing`.
- `aws-chunked` de-chunking unit test (framed input → raw bytes).
- HTTP handler tests using Netty's `EmbeddedChannel` (no sockets) against a hand-written fake
  `CandyboxClient` seam (extract a small interface the handler depends on, fake it in tests —
  consistent with the SPI+fake pattern).

**Integration tests (`candybox-integration-tests`, `*IT`, `mvn verify`):**
- `S3GatewayIT`: boot embedded BookKeeper + in-JVM ZooKeeper + a real Candybox node + the gateway, then
  drive it with an **HTTP client** (and ideally the **AWS SDK for Java v2** pointed at the gateway with
  path-style + anonymous credentials) through create-bucket → put → head → get → list (with delimiter
  + pagination) → copy → delete → delete-bucket. Assert bytes round-trip and ETag is stable.
- A negative-path `*IT` covering the error mappings end-to-end (missing key/bucket, non-empty bucket).
- Respect the existing IT conventions: `*IT` suffix, failsafe binding, `reuseForks=false`, and the
  `--add-opens` `argLine`.

---

## 14. Milestones / effort ordering

1. **Skeleton:** module + Netty bootstrap + config + health server + `CandyboxClient` (cluster mode)
   wiring + graceful shutdown.
2. **Error/XML framework:** `S3Error`, `ErrorMapper`, `S3XmlWriter`, `RequestId`.
3. **Bucket ops:** create/delete/head/list + their XML and error mappings. First end-to-end `*IT`.
4. **Object ops (single-shot):** PutObject (streaming bridge + aws-chunked de-chunk), GetObject
   (streaming), HeadObject, DeleteObject + ETag from crc32c.
5. **Listing:** ListObjectsV2/V1, pagination, delimiter/CommonPrefixes rollup.
6. **CopyObject + DeleteObjects (batch).**
7. **SDK shims:** canned subresources, `Expect: 100-continue`, ignore-auth, docs for client config.
8. **Packaging:** dist launcher, Docker third mode, Compose + K8s `Deployment` examples.

---

## 15. Remaining minor decisions (sensible defaults chosen; flag to revisit)

- **Bucket `CreationDate`:** Candybox stores none. Default: return process-start/epoch placeholder.
  (A real value needs a bucket-metadata field — minor future work.)
- **`DeleteObjects` strategy:** default to a per-key `deleteCandy` loop; optionally detect a pure-prefix
  batch and use `deleteRange` as an optimization later.
- **`max-keys` clamp:** default cap 1000 (S3 parity).
- **Continuation token opacity:** base64-encode `nextStartAfter` so clients treat it as opaque.
- **Cross-bucket `CopyObject`:** v1 returns `501 NotImplemented` (Candybox `copyCandy` is intra-Box);
  revisit if needed.

---

## 16. Deferred features (future plans)

These were intentionally deferred from v1 because each requires **new Candybox capabilities**, not just
gateway code:

1. **Multipart upload.** Requires *native multipart support in Candybox* — a way to stage parts and
   atomically assemble them into one object (ideally reusing Syrup/zero-copy mechanics) plus an
   upload-id/parts registry. Until then, the gateway cannot faithfully implement
   `CreateMultipartUpload`/`UploadPart`/`CompleteMultipartUpload`/`AbortMultipartUpload`, and large
   uploads must use single-shot PUT under `s3.max-object-bytes`. When this lands, the gateway gains the
   four MPU operations and the composite `-N` ETag form.

2. **Real MD5 ETag + checksum verification.** Requires a *new object-metadata layer* to compute and
   persist the MD5 digest (and optionally validate `Content-MD5` / `x-amz-checksum-*` on PUT).
   Candybox currently stores only CRC32C, hence the deterministic-from-CRC32C interim ETag (§8). When
   the metadata layer exists, `Etag.java` swaps to the stored MD5 and the gateway can enforce
   `Content-MD5` and the SDK checksum headers. (MPU's composite ETag depends on this too.)

3. **Range GET (HTTP 206).** Requires an *engine/protocol byte-window read* so the gateway can fetch
   `[offset, offset+len)` directly from the Syrup instead of streaming-and-discarding. Until then v1
   ignores `Range` and returns the full object (HTTP 200). When the engine read lands, add 206 +
   `Content-Range` + multi-range support.

4. **Other S3 surface (further out):** virtual-host addressing (needs wildcard DNS/cert at the LB),
   SigV4 authentication + a credential store, object versioning (the LSM tree is LWW today),
   bucket policies/ACLs/CORS/lifecycle/tagging, and an engine-level delimiter scan to make
   `CommonPrefixes` cheap.
```
