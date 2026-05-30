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
 * Thrown when a request is structurally invalid (bad name, malformed key, etc.). Validated at the
 * client (fail fast) and re-validated at the node (authoritative).
 */
public class ValidationException extends CandyboxException {

    public ValidationException(String message) {
        super(message);
    }
}
