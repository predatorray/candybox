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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ExceptionsExecuteAndGetExceptionTest<T extends Exception> {

    private static final RuntimeException EX1 = new RuntimeException();

    private final Exceptions.Executable<T> executable;
    private final Class<T> exClz;
    private final T expectedException;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {(Exceptions.Executable<RuntimeException>) () -> {throw EX1;}, RuntimeException.class, EX1},
                {(Exceptions.Executable<RuntimeException>) () -> {}, RuntimeException.class, null}
        });
    }

    public ExceptionsExecuteAndGetExceptionTest(Exceptions.Executable<T> executable,
                                                                      Class<T> exClz, T expectedException) {
        this.executable = executable;
        this.exClz = exClz;
        this.expectedException = expectedException;
    }

    @Test
    public void exercise() {
        Optional<T> result = Exceptions.executeAndGetException(executable, exClz);
        assertEquals(Optional.ofNullable(expectedException), result);
    }
}