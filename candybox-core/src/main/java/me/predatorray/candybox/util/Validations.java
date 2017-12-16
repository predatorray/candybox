/*
 * Copyright (c) 2017 the original author or authors.
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

package me.predatorray.candybox.util;

public class Validations {

    /**
     * Validate if the object is not null, throws {@link IllegalArgumentException} if null.
     * @param obj the object to be validated
     * @param <T> the type of the object
     * @return the validated object
     * @throws IllegalArgumentException if the object is null
     */
    public static <T> T notNull(T obj) throws IllegalArgumentException {
        if (obj == null) {
            throw new IllegalArgumentException();
        }
        return obj;
    }
}
