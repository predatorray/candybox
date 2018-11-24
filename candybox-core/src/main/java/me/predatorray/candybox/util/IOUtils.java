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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * Common IO utilities
 *
 * @author Wenhao Ji
 */
public class IOUtils {

    /**
     * Close the closeable instance if not null and suppress any IOException occurred during the
     * {@link Closeable#close()} invocation if the throwable is present.
     *
     * @param closeable the instance to be closed (nullable)
     * @param throwable the more important throwable (nullable)
     * @param <T> the type of the throwable
     * @throws T the same as the {@code throwable} argument, if it is present
     */
    public static <T extends Throwable> void closeAndSuppress(Closeable closeable, T throwable) throws T {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException suppressed) {
            if (throwable != null) {
                throwable.addSuppressed(suppressed);
            }
        }
        if (throwable != null) {
            throw throwable;
        }
    }

    public static Short readShortOrNone(DataInputStream in) throws IOException {
        int ch1 = in.read();
        if (ch1 < 0) {
            return null;
        }
        int ch2 = in.read();
        if (ch2 < 0) {
            throw new EOFException();
        }
        return (short) ((ch1 << 8) + (ch2 << 0));
    }
}
