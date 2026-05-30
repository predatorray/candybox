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
 * Thrown (and surfaced on the wire as a {@code BUSY} response) when a node is under write-stall
 * backpressure — e.g. too many L0 SSTables awaiting compaction. The operation is retriable; the
 * client should back off rather than treat it as a hard failure.
 */
public class BusyException extends CandyboxException {

    public BusyException(String message) {
        super(message);
    }
}
