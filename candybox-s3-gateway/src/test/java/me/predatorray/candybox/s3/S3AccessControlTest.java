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

import java.util.EnumSet;
import me.predatorray.candybox.common.auth.Grant;
import me.predatorray.candybox.common.auth.Operation;
import org.junit.jupiter.api.Test;

class S3AccessControlTest {

    @Test
    void fullControlMapsExactlyTheFourS3Permissions() {
        EnumSet<Operation> permissions = S3AccessControl.s3Permission("FULL_CONTROL");
        assertThat(permissions).containsExactlyInAnyOrder(
                Operation.READ, Operation.WRITE, Operation.READ_ACP, Operation.WRITE_ACP);
        assertThat(S3AccessControl.s3PermissionNames(new Grant("User:bob", permissions)))
                .containsExactly("FULL_CONTROL");
        // Each translation result is independent of mutations by its caller.
        permissions.clear();
        assertThat(S3AccessControl.s3Permission("FULL_CONTROL")).hasSize(4);
    }

    @Test
    void nativeAdministrationDoesNotChangeS3Rendering() {
        assertThat(S3AccessControl.s3PermissionNames(
                new Grant("User:bob", EnumSet.allOf(Operation.class))))
                .containsExactly("FULL_CONTROL");
        assertThat(S3AccessControl.s3PermissionNames(Grant.of("User:bob", Operation.ADMIN)))
                .isEmpty();
        assertThat(S3AccessControl.s3PermissionNames(
                Grant.of("User:bob", Operation.READ, Operation.ADMIN)))
                .containsExactly("READ");
    }
}
