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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class S3RouterUnsupportedTest {
    @ParameterizedTest
    @CsvSource({"POST, photos, key", "POST, photos,", "PATCH, photos, key",
        "BREW, photos, key", "BREW,,"})
    void unsupportedMethodsAndPlainPosts(String method, String bucket, String key) {
        assertThat(S3Router.route(method, bucket, key, Set.of()))
                .isEqualTo(S3Router.S3Action.UNSUPPORTED);
    }

    @ParameterizedTest
    @CsvSource({
        "PUT, partnumber:uploadid, UPLOAD_PART", "POST, uploadid, COMPLETE_MULTIPART_UPLOAD",
        "POST, uploads, CREATE_MULTIPART_UPLOAD", "GET, uploadid, LIST_PARTS",
        "DELETE, uploadid, ABORT_MULTIPART_UPLOAD"
    })
    void missingKeyMultipartQueriesSelectObjectActions(String method, String queries,
                                                       S3Router.S3Action expected) {
        Set<String> names = Set.of(queries.split(":"));
        assertThat(S3Router.route(method, "photos", null, names)).isEqualTo(expected);
        assertThat(S3Router.route(method, "photos", "", names)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
        "PUT, acl, PUT_BUCKET_ACL", "POST, delete, DELETE_OBJECTS",
        "GET, uploads, LIST_MULTIPART_UPLOADS", "GET, acl, GET_BUCKET_ACL",
        "GET, location, GET_BUCKET_LOCATION", "GET, versioning, GET_BUCKET_VERSIONING",
        "GET, versions, LIST_OBJECT_VERSIONS"
    })
    void bucketSubresourcesKeepTheirActions(String method, String query, S3Router.S3Action expected) {
        assertThat(S3Router.route(method, "photos", null, Set.of(query))).isEqualTo(expected);
        assertThat(S3Router.route(method, "photos", "", Set.of(query))).isEqualTo(expected);
    }
}
