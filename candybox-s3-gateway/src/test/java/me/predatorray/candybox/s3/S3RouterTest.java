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
import me.predatorray.candybox.s3.S3Router.S3Action;
import org.junit.jupiter.api.Test;

class S3RouterTest {

    @Test
    void serviceRoot() {
        assertThat(S3Router.route("GET", null, null, Set.of())).isEqualTo(S3Action.LIST_BUCKETS);
        assertThat(S3Router.route("POST", null, null, Set.of())).isEqualTo(S3Action.UNSUPPORTED);
    }

    @Test
    void bucketOps() {
        assertThat(S3Router.route("PUT", "photos", null, Set.of())).isEqualTo(S3Action.CREATE_BUCKET);
        assertThat(S3Router.route("DELETE", "photos", "", Set.of())).isEqualTo(S3Action.DELETE_BUCKET);
        assertThat(S3Router.route("HEAD", "photos", null, Set.of())).isEqualTo(S3Action.HEAD_BUCKET);
        assertThat(S3Router.route("GET", "photos", null, Set.of())).isEqualTo(S3Action.LIST_OBJECTS);
        assertThat(S3Router.route("POST", "photos", null, Set.of("delete")))
                .isEqualTo(S3Action.DELETE_OBJECTS);
    }

    @Test
    void bucketSubresources() {
        assertThat(S3Router.route("GET", "photos", null, Set.of("location")))
                .isEqualTo(S3Action.GET_BUCKET_LOCATION);
        assertThat(S3Router.route("GET", "photos", null, Set.of("versioning")))
                .isEqualTo(S3Action.GET_BUCKET_VERSIONING);
        assertThat(S3Router.route("GET", "photos", null, Set.of("acl")))
                .isEqualTo(S3Action.GET_BUCKET_ACL);
    }

    @Test
    void objectOps() {
        assertThat(S3Router.route("PUT", "photos", "cat.jpg", Set.of())).isEqualTo(S3Action.PUT_OBJECT);
        assertThat(S3Router.route("GET", "photos", "cat.jpg", Set.of())).isEqualTo(S3Action.GET_OBJECT);
        assertThat(S3Router.route("HEAD", "photos", "cat.jpg", Set.of())).isEqualTo(S3Action.HEAD_OBJECT);
        assertThat(S3Router.route("DELETE", "photos", "a/b/c.jpg", Set.of()))
                .isEqualTo(S3Action.DELETE_OBJECT);
    }

    @Test
    void unsupportedMethod() {
        assertThat(S3Router.route("PATCH", "photos", "cat.jpg", Set.of())).isEqualTo(S3Action.UNSUPPORTED);
    }
}
