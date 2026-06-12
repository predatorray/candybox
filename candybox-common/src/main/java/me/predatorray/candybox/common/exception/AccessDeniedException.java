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
package me.predatorray.candybox.common.exception;

/**
 * The caller authenticated fine but is not authorized for the operation (maps to S3's
 * {@code AccessDenied} / HTTP 403). Distinct from {@link AuthenticationException}, which is about
 * who the caller is.
 */
public class AccessDeniedException extends CandyboxException {

    public AccessDeniedException(String message) {
        super(message);
    }
}
