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

import java.util.function.Predicate;

/**
 * Utilities for arguments validations
 *
 * @author Wenhao Ji
 */
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
            throw new IllegalArgumentException("null");
        }
        return obj;
    }

    /**
     * Validate if the number is positive (n &gt; 0), throws {@link IllegalArgumentException} if not.
     * @param n the number to be validated
     * @return the validated number
     * @throws IllegalArgumentException if the number is zero or negative
     */
    public static long positive(long n) throws IllegalStateException {
        if (n <= 0) {
            throw new IllegalArgumentException("A positive number is required: " + n);
        }
        return n;
    }

    /**
     * Validate if the number is non-negative (n &gt;= 0), throws {@link IllegalArgumentException} if not.
     * @param n the number to be validated
     * @return the validated number
     * @throws IllegalArgumentException if the number is negative
     */
    public static long nonnegative(long n) throws IllegalStateException {
        if (n < 0) {
            throw new IllegalArgumentException("A non-negative number is required: " + n);
        }
        return n;
    }

    /**
     * Validate if the object satisfy the condition, throws {@link IllegalArgumentException} if not.
     * @param obj the object to be validated
     * @param condition the condition that the object must satisfy
     * @param message the error message thrown when the object does not satisfy the condition
     * @param <T> the type of the object
     * @return the validated object
     * @throws IllegalArgumentException if the condition is not satisfied (false)
     */
    public static <T> T that(T obj, boolean condition, String message) throws IllegalArgumentException {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
        return obj;
    }

    /**
     * Validate if the object satisfy the condition, throws {@link IllegalArgumentException} if not.
     * @param obj the object to be validated
     * @param condition the condition function that the object must satisfy.
     *                  The function returns true case satisfied, false otherwise.
     * @param message the error message thrown when the object does not satisfy the condition
     * @param <T> the type of the object
     * @return the validated object
     * @throws IllegalArgumentException if the condition is not satisfied (false)
     */
    public static <T> T that(T obj, Predicate<? super T> condition, String message) throws IllegalArgumentException {
        if (!condition.test(obj)) {
            throw new IllegalArgumentException(message);
        }
        return obj;
    }
}
