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
package me.predatorray.candybox.s3;

import java.util.Set;

/**
 * Pure routing: classifies a request from its method, the parsed {@code bucket}/{@code key}, and the
 * set of query-parameter names into an {@link S3Action}. Path-style addressing only (the bucket is the
 * first path segment; the {@code Host} header is irrelevant). PUT-vs-copy is decided later from the
 * {@code x-amz-copy-source} header, so both classify as {@link S3Action#PUT_OBJECT} here.
 *
 * <p>Kept free of Netty types so it can be unit-tested directly. See {@code S3_GATEWAY_PLAN.md} §5.
 */
final class S3Router {

    enum S3Action {
        LIST_BUCKETS,
        CREATE_BUCKET,
        DELETE_BUCKET,
        HEAD_BUCKET,
        LIST_OBJECTS,
        LIST_OBJECT_VERSIONS,
        DELETE_OBJECTS,
        GET_BUCKET_LOCATION,
        GET_BUCKET_VERSIONING,
        GET_BUCKET_ACL,
        LIST_MULTIPART_UPLOADS,
        PUT_OBJECT,
        GET_OBJECT,
        HEAD_OBJECT,
        DELETE_OBJECT,
        CREATE_MULTIPART_UPLOAD,
        UPLOAD_PART,
        COMPLETE_MULTIPART_UPLOAD,
        ABORT_MULTIPART_UPLOAD,
        LIST_PARTS,
        UNSUPPORTED
    }

    private S3Router() {
    }

    /**
     * @param method  the HTTP method name (upper-case: GET/PUT/HEAD/DELETE/POST)
     * @param bucket  the parsed bucket, or {@code null} for the service root ("/")
     * @param key     the parsed object key, or {@code null} if the request targets a bucket
     * @param queries the set of query-parameter names present (lower-cased)
     */
    static S3Action route(String method, String bucket, String key, Set<String> queries) {
        if (bucket == null) {
            return "GET".equals(method) ? S3Action.LIST_BUCKETS : S3Action.UNSUPPORTED;
        }
        if (key == null || key.isEmpty()) {
            return routeBucket(method, queries);
        }
        return switch (method) {
            case "PUT" -> {
                // PUT ?partNumber&uploadId is UploadPart; otherwise it's PUT_OBJECT (copy is decided
                // from x-amz-copy-source later).
                if (queries.contains("partnumber") && queries.contains("uploadid")) {
                    yield S3Action.UPLOAD_PART;
                }
                yield S3Action.PUT_OBJECT;
            }
            case "GET" -> {
                if (queries.contains("uploadid")) {
                    yield S3Action.LIST_PARTS;
                }
                yield S3Action.GET_OBJECT;
            }
            case "HEAD" -> S3Action.HEAD_OBJECT;
            case "DELETE" -> {
                if (queries.contains("uploadid")) {
                    yield S3Action.ABORT_MULTIPART_UPLOAD;
                }
                yield S3Action.DELETE_OBJECT;
            }
            case "POST" -> {
                if (queries.contains("uploads")) {
                    yield S3Action.CREATE_MULTIPART_UPLOAD;
                }
                if (queries.contains("uploadid")) {
                    yield S3Action.COMPLETE_MULTIPART_UPLOAD;
                }
                yield S3Action.UNSUPPORTED;
            }
            default -> S3Action.UNSUPPORTED;
        };
    }

    private static S3Action routeBucket(String method, Set<String> queries) {
        return switch (method) {
            case "PUT" -> S3Action.CREATE_BUCKET;
            case "DELETE" -> S3Action.DELETE_BUCKET;
            case "HEAD" -> S3Action.HEAD_BUCKET;
            case "POST" -> queries.contains("delete") ? S3Action.DELETE_OBJECTS : S3Action.UNSUPPORTED;
            case "GET" -> {
                if (queries.contains("location")) {
                    yield S3Action.GET_BUCKET_LOCATION;
                }
                if (queries.contains("versioning")) {
                    yield S3Action.GET_BUCKET_VERSIONING;
                }
                if (queries.contains("acl")) {
                    yield S3Action.GET_BUCKET_ACL;
                }
                if (queries.contains("versions")) {
                    yield S3Action.LIST_OBJECT_VERSIONS;
                }
                if (queries.contains("uploads")) {
                    yield S3Action.LIST_MULTIPART_UPLOADS;
                }
                yield S3Action.LIST_OBJECTS;
            }
            default -> S3Action.UNSUPPORTED;
        };
    }
}
