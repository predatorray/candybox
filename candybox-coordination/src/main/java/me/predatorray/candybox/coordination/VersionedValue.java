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
package me.predatorray.candybox.coordination;

/**
 * A value plus the version that produced it, the unit of optimistic concurrency for coordination
 * keys (notably the per-Box manifest pointer). The version increments on every successful write and
 * is the {@code expectedVersion} a compare-and-set must match.
 *
 * @param version the current version (starts at 0 on create)
 * @param value   the stored bytes (treat as read-only)
 */
public record VersionedValue(long version, byte[] value) {
}
