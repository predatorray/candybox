# Multipart Upload + Range GET — Design and Implementation Plan

This document records the agreed design for adding **Range GET** (`HTTP 206`) and **multipart
upload** (`CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload` /
`AbortMultipartUpload`) to Candybox. It supersedes the "deferred" lines for these two features in
[`S3_GATEWAY_PLAN.md`](S3_GATEWAY_PLAN.md) §16 and the `// TODO(phase-2)` note on streaming in
[`DESIGN.md`](DESIGN.md) §12. The system is not yet used in production, so a clean format break
is acceptable.

## 1. Why the data model has to change

A single `CandyLocator` today carries `(contentLength, chunkSize, crc32c, segments[])` where every
chunk in the object is exactly `chunkSize` bytes (modulo the very last one). Byte-offset → entry is
the arithmetic `o / chunkSize` over a flat `segments` list — uniform chunking is what makes that
arithmetic legal.

Multipart breaks the uniformity. If part A is 1.5 MiB and part B is 1.5 MiB with `chunkSize = 1
MiB`, the concatenated bytes have chunks of sizes `1, 0.5, 1, 0.5` MiB — a short-tail chunk appears
in the *middle* of the object, which the flat layout cannot address. Solutions that keep the flat
layout (rewriting tails into uniform chunks at `Complete`, padding parts, etc.) defeat the whole
point of multipart by re-copying the bytes.

So the locator becomes **a list of parts**, each of which is internally what today's whole locator
is. Range GET technically does not require this change — the current uniform-chunked layout is
range-addressable today — but the part model is a cleaner foundation for it too, and shipping
both features on the same data model avoids a second format break later.

## 2. Locked design decisions

| | |
|---|---|
| **Locator shape** | `CandyLocator { hlc, type, contentType, userMetadata, createdAtMillis, parts: List<Part> }` where `Part { partLength, chunkSize, crc32c, segments: List<SegmentRef> }`. Single-PUT and `copy/rename` produce a 1-part locator; multipart produces N parts; tombstones have an empty list. |
| **Format version** | `CandyLocatorSerializer` bumps to v2. `MutationSerializer` and `SSTableFormat` footer are **not** bumped — block layout/index/bloom/range-tomb structure is locator-opaque. Old data is regenerated in tests (no backward compat). |
| **Locator size cap** | Raised from 64 KiB → **256 KiB**, so a 10,000-part object fits (S3 parity). A 10k-part locator is ~200 KiB; SSTable data-block target stays ~64 KiB but a fat locator simply spans a larger entry. |
| **Pending-upload state** | Lives in a new manifest section `multipartUploads: Map<uploadId, UploadState>`. Replayed on handover; every Create/UploadPart/Complete/Abort is a fencing-gated manifest edit. No new ledger role, no scratch keyspace, listings are unaffected. |
| **Atomicity of Complete** | One write-lock-held sequence: build assembled `CandyLocator`, append `Mutation(targetKey, locator)` to WAL, apply manifest edit that drops the upload entry, apply to memtable. Fencing-gated; idempotent via the existing idempotency-token cache. |
| **HLC stamping for parts** | Only `Complete` stamps the durable HLC. Per-part metadata in the manifest section is bookkeeping and does not interact with LWW. |
| **Last-write-wins per part** | Re-uploading the same `partNumber` under one `uploadId` supersedes the previous part. Superseded segments enter the existing `pendingOrphanSyrups` path. |
| **Whole-object CRC** | Removed from the locator. Each part keeps its own end-to-end CRC32C. Full reads validate per-part; partial reads validate per-chunk. ETag is gateway-defined and unchanged in concept. |
| **Per-part minimum** | **5 MiB** for every part except the last (S3 parity), configurable. |
| **Range syntax** | `bytes=A-B`, `bytes=A-`, `bytes=-N`. Multi-range (`bytes=A-B,C-D`) is **rejected** — multipart/byteranges responses are out of scope for v1. |
| **Wire streaming** | Still buffered. Per-part body is bounded by the 16 MiB frame cap — well under S3's 5 GiB. Multipart is decoupled from the streaming `TODO(phase-2)`. |
| **Abort TTL sweep** | Background reaper on the compaction/GC worker; **7-day** default, configurable. Explicit `Abort` always wins. |
| **UploadPartCopy** | Same-Box only, mirroring the existing `copyCandy`/`renameCandy` rule. Cross-bucket copy stays `NOT_IMPLEMENTED`. |
| **Concurrency** | All multipart ops go through the engine's write lock (same as PUT). Parts within an upload can be uploaded by the client in any order; the server serializes them per Box. |

## 3. Read path

`BoxEngine.resolveLive` still returns `Optional<CandyLocator>`; only the shape changes. Full reads
walk `locator.parts()` and reassemble each part as today. Range reads use a small offset table
(prefix-sum of part lengths) to find the first/last part, then index into chunks by `byteWithinPart
/ part.chunkSize()`. Per-chunk CRC validates on every read; whole-object CRC no longer exists.

## 4. Write path — single PUT vs. multipart

- **Single PUT** is unchanged from the client's perspective; the engine wraps the result of
  `SyrupManager.writeCandy(...)` in a 1-element `parts` list and writes one mutation.
- **Multipart**:
  - `CreateMultipartUpload(box, key, contentType, userMetadata)` → `uploadId`. Records
    `{uploadId, createdAtMillis, contentType, userMetadata, parts: {}}` in the manifest's
    `multipartUploads` section under one fenced edit. Generated `uploadId` is a 128-bit random
    base32-encoded string; uniqueness is per-Box.
  - `UploadPart(box, key, uploadId, partNumber, bytes)` → per-part CRC32C. Streams bytes through
    `SyrupManager.writeCandy` (one Syrup run, may roll Syrups), then appends a manifest edit
    `(uploadId, partNumber → Part)`. If the same `partNumber` already had a Part, the prior Part's
    segments are enqueued to `pendingOrphanSyrups` (after the manifest edit is committed).
  - `CompleteMultipartUpload(box, key, uploadId, [{partNumber, etagOrCrc}, ...])`:
    1. Validate the list is strictly part-number-ordered and that each entry matches the recorded
       part (length + CRC).
    2. Validate every part except the last is ≥ `multipartMinPartBytes`.
    3. Assemble `CandyLocator(type=PUT, parts=[partInOrder...])` under a fresh HLC.
    4. WAL-append the mutation, memtable-apply it, and apply a manifest edit that removes the
       upload entry — all under one write lock, fencing-gated.
    5. The composed Candy is now indistinguishable from a single-PUT Candy with multiple parts.
  - `AbortMultipartUpload(box, key, uploadId)` → one manifest edit removing the entry; all parts'
    Syrup segments enter the pending-orphan path for GC.

## 5. Wire protocol additions

New opcodes (no streaming yet; bodies inlined in one frame):

| Opcode | Request → Response |
|---|---|
| `RANGE_GET_CANDY` | `(box, key, firstByte, lastByte)` → `CandyDataResponse(contentLength=slice, ..., data=slice)` |
| `CREATE_MULTIPART_UPLOAD` | `(box, key, contentType, userMetadata)` → `(uploadId)` |
| `UPLOAD_PART` | `(box, key, uploadId, partNumber, data)` → `(partCrc32c, partLength)` |
| `COMPLETE_MULTIPART_UPLOAD` | `(box, key, uploadId, [{partNumber, crc32c}], idempotencyToken)` → `CandyMetadata` |
| `ABORT_MULTIPART_UPLOAD` | `(box, key, uploadId)` → `Ok` |
| `LIST_MULTIPART_UPLOADS` | `(box, prefix, keyMarker, uploadIdMarker, maxUploads)` → list |
| `LIST_PARTS` | `(box, key, uploadId, partNumberMarker, maxParts)` → list |
| `UPLOAD_PART_COPY` | `(box, key, uploadId, partNumber, srcKey, firstByte, lastByte)` → `(partCrc32c, partLength)` |

`GetObject?partNumber=N` is a special-case of Range GET handled by the gateway (compute the byte
window for part N from the headed locator), so it doesn't need a dedicated opcode in v1.

## 6. S3 gateway routes

Adds to [`candybox-s3-gateway`](candybox-s3-gateway):

| Method + path + query | Action |
|---|---|
| `GET /b/k` + `Range: bytes=…` | Range GET → 206 + `Content-Range` |
| `GET /b/k?partNumber=N` | Range GET sized to part N |
| `POST /b/k?uploads` | CreateMultipartUpload → `InitiateMultipartUploadResult` XML |
| `PUT  /b/k?partNumber=N&uploadId=…` | UploadPart → `ETag` header (CRC32C hex, like single PUT) |
| `PUT  /b/k?partNumber=N&uploadId=…` + `x-amz-copy-source` | UploadPartCopy (same-Box only) |
| `POST /b/k?uploadId=…` (XML body) | CompleteMultipartUpload → `CompleteMultipartUploadResult` XML |
| `DELETE /b/k?uploadId=…` | AbortMultipartUpload → 204 |
| `GET /b/?uploads` | ListMultipartUploads → XML |
| `GET /b/k?uploadId=…` | ListParts → XML |

ETag for a multipart object is `crc32c-N` (suffix is the part count) — the gateway computes it from
the part CRCs (analog of S3's `MD5(MD5||MD5||…)-N`).

## 7. Garbage collection

`BoxEngine.recomputeOrphanSyrupsLocked` already walks the manifest's referenced Syrups + the
memtable. With locator v2 the walk flattens `parts→segments`. Additionally, pending uploads in
`multipartUploads` are sources of Syrup references — included so their parts are not GC'd while in
flight. Abort/Complete updates the reference set under the fencing-gated manifest edit, and the
existing pending-orphan path reclaims orphaned Syrups after the grace period.

The compaction/GC worker grows a new step: scan `multipartUploads` for entries whose `createdAtMillis`
plus `multipartUploadTtlMillis` (default 7 days) is in the past and treat them as if `Abort` had
been called.

## 8. Configuration additions

To `CandyboxConfig` (with defaults):

| Key | Default | Meaning |
|---|---|---|
| `sizeLimits.maxLocatorBytes` | 256 KiB | Raised from 64 KiB. |
| `multipart.minPartBytes` | 5 MiB | Minimum size for every part except the last. |
| `multipart.maxParts` | 10000 | Hard cap on a single upload. |
| `multipart.uploadTtlMillis` | 7d | Grace before a pending upload is auto-aborted. |
| `multipart.maxConcurrentUploadsPerBox` | 10000 | Defensive cap so a misbehaving client cannot exhaust the manifest. |

## 9. Implementation plan (sequenced PRs)

1. **Locator v2 + `Part` record** — `Part`, `CandyLocator(parts)`, `CandyLocatorSerializer` v2,
   `MutationSerializer` (no change), `Memtable`/`SSTableReader`/`SSTableWriter` (no change beyond
   the type), GC reference-count walk (flattens parts→segments). All existing flows produce
   1-part locators. Existing tests regenerate golden bytes. **No new features yet.**
2. **Range GET** — `SyrupReader.readRange(parts, firstByte, lastByte, out)`,
   `BoxEngine.getCandyRange(...)`, `RANGE_GET_CANDY` opcode + `CandyboxClient.getCandyRange`,
   gateway `Range:` parsing returning 206 + `Content-Range`. Reject multi-range.
3. **Manifest `multipartUploads` section + handover replay** — `ManifestEdit` additions,
   `ManifestState.uploads()`, `ManifestSerializer` carries them, handover replays them. Contract
   test: an in-flight upload survives a fenced takeover.
4. **BoxEngine multipart API** — `createMultipartUpload`/`uploadPart`/`completeMultipartUpload`/
   `abortMultipartUpload`, supersede + abort orphan-Syrup wiring, validation of part order/sizes,
   Complete writes the assembled mutation through the existing WAL+memtable path.
5. **Protocol + client + gateway endpoints** — opcodes + codec; `CandyboxClient` methods;
   `S3Router` + `S3Handler` for POST?uploads, PUT?partNumber&uploadId, POST?uploadId(XML),
   DELETE?uploadId, GET?partNumber=N; new `S3Xml` builders.
6. **ListMultipartUploads / ListParts + UploadPartCopy** — read endpoints over the manifest upload
   section; same-Box `UploadPartCopy` (locator-level part-slice clone via the existing zero-copy
   primitives).
7. **TTL sweep + docs** — background reaper on the compaction/GC worker; update `DESIGN.md`
   §5/§9/§10/§12, `S3_GATEWAY_PLAN.md` §16, `README.md` calibration line.

## 10. Open knobs (defaults already picked; calling them out for visibility)

- **Whole-object CRC32C** is dropped (per-part CRC suffices).
- **Multi-range** is rejected.
- **HLC** is stamped only on Complete; per-part records are pure bookkeeping.
- **UploadPartCopy** is same-Box only.
- **`SSTableFormat` footer** is not bumped.
- **Wire streaming** stays `TODO(phase-2)`; per-part body capped by the 16 MiB frame cap.

## 11. Out of scope for this work

- Versioning (`x-amz-version-id`, `?versioning` API).
- POST Object form uploads, SSE, ACLs, lifecycle, CORS, SigV4.
- True wire streaming (`TODO(phase-2)`).
- Distributed cross-node compaction scheduling (`TODO(phase-3)`).

These remain as-is in `S3_GATEWAY_PLAN.md`/`DESIGN.md`.
