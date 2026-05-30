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
 * Thrown when a {@code compareAndSet} (or versioned delete) sees a version other than the one
 * expected — a concurrent writer won the race. The caller must re-read and retry; it must never
 * fall back to a blind set, which would clobber a concurrent manifest checkpoint.
 */
public class CasConflictException extends CoordinationException {

    public CasConflictException(String key, long expectedVersion, long actualVersion) {
        super("CAS conflict on key '" + key + "': expected version " + expectedVersion
                + " but found " + actualVersion);
    }
}
