/*
 * Copyright (c) 2018 the original author or authors.
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

import java.util.Optional;

public class Exceptions {

    public static <T extends Exception> Optional<T> executeAndGetException(Executable<T> executable, Class<T> exClz) {
        try {
            executable.execute();
        } catch (Exception e) {
            if (exClz.isInstance(e)) {
                return Optional.of(exClz.cast(e));
            } else {
                throw new UnexpectedException(e);
            }
        }
        return Optional.empty();
    }

    public static class UnexpectedException extends RuntimeException {

        public UnexpectedException(Exception unexpected) {
            super(unexpected);
        }
    }

    @FunctionalInterface
    public interface Executable<T extends Exception> {

        void execute() throws T;
    }
}
