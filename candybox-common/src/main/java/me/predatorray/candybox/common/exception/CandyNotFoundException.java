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

/** Thrown when reading a Candy key that has no live value (never written, or tombstoned). */
public class CandyNotFoundException extends CandyboxException {

    public CandyNotFoundException(String boxName, String candyKey) {
        super("No such Candy: box=" + boxName + " key=" + candyKey);
    }
}
