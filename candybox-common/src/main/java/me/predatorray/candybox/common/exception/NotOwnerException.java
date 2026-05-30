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
 * Thrown when a node is asked to serve a Box it does not currently own (its ownership lease has not
 * been acquired, or has expired/been superseded). In a cluster the client should re-resolve the
 * Box's owner and retry; Phase 2 routing surfaces this as a {@code MOVED} response.
 */
public class NotOwnerException extends CandyboxException {

    public NotOwnerException(String boxName) {
        super("This node does not own Box: " + boxName);
    }
}
