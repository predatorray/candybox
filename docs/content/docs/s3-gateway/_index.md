---
title: S3 gateway
weight: 50
---

# S3 gateway

The `candybox-s3-gateway` module is a **path-style, S3-compatible HTTP gateway** built on Netty that
translates the S3 REST/XML API onto the Candybox client. It is stateless and runs behind an HTTP(S)
load balancer. In the bundled Compose stack it listens on `:9711`.

It supports optional **SigV4 authentication** and **S3 ACL enforcement**, so multi-user and
cross-account access patterns work the way S3 SDKs expect.

## Using it from an S3 SDK

Point any S3 client at the gateway endpoint with path-style addressing. For example, with the AWS CLI:

```bash
aws --endpoint-url http://localhost:9711 s3 mb s3://photos
echo 'hello candybox' | aws --endpoint-url http://localhost:9711 s3 cp - s3://photos/hello.txt
aws --endpoint-url http://localhost:9711 s3 ls s3://photos/
```

## Range GET and multipart upload

Object reads accept HTTP `Range: bytes=A-B` (also `bytes=A-` and `bytes=-N`) and return `206 Partial
Content` with the right `Content-Range`; multi-range requests are rejected.

Multipart upload is fully wired through the gateway: `CreateMultipartUpload` / `UploadPart` /
`CompleteMultipart` / `AbortMultipartUpload`, plus `UploadPartCopy` and `ListMultipartUploads` /
`ListParts`. Background TTL sweeps abandon stale uploads after `multipart.upload.ttl.millis` (7 days by
default).

## Compatibility

The gateway's S3 compatibility is verified against the industry-standard
[`ceph/s3-tests`](https://github.com/ceph/s3-tests) suite. The latest calibration runs the gateway
with **SigV4 auth + S3 ACLs enabled**: **192 / 838** boto3 functional tests pass, zero suite errors.
The extra passes over an unauthenticated gateway are the multi-user / ACL / cross-account-access tests
that real authentication unlocks (`bucket_acl_*`, `object_acl_*`, `access_bucket_*`, anonymous-access
and bad-auth checks).

The remaining gaps the v1 gateway does not yet implement are **versioning, SSE, POST object,
lifecycle, bucket policy, CORS, and conditional GET**. See
[`compat/s3-tests/README.md`](https://github.com/predatorray/candybox/blob/main/compat/s3-tests/README.md)
for the family-by-family breakdown, and
[`S3_GATEWAY_PLAN.md`](https://github.com/predatorray/candybox/blob/main/S3_GATEWAY_PLAN.md) /
[`AUTH_PLAN.md`](https://github.com/predatorray/candybox/blob/main/AUTH_PLAN.md) for the design.
