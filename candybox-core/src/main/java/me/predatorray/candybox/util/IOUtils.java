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
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collection;

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

    /**
     * Close all the closeable(s) sequentially even IOException occurs while being {@link Closeable#close()}-ed.
     *
     * <p>The first IOException will be thrown as the root cause while any others will be suppressed
     * by using method {@link Throwable#addSuppressed(Throwable)}.
     *
     * <p><b>NOTE THAT</b>, only {@link IOException}s will be properly handled. Any other type of exceptions or throwables
     * will be left uncaught and thrown immediately.
     *
     * @param closeables a collection of instances that are closeable
     * @throws IOException when ny of the instances in the collection throws IOException while being closed
     */
    public static void closeSequentially(Collection<? extends Closeable> closeables) throws IOException {
        IOException rootCause = null;
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                if (rootCause == null) {
                    rootCause = e;
                } else {
                    rootCause.addSuppressed(e);
                }
            }
        }
        if (rootCause != null) {
            throw rootCause;
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

    public static <T extends Throwable> T addSuppressIfThrown(T nontrivial, Closeable closeable) {
        Exceptions.executeAndGetException(closeable::close, Exception.class).ifPresent(nontrivial::addSuppressed);
        return nontrivial;
    }

    public static void truncate(Path path, long newSize) throws IOException {
        try (FileChannel channel = new FileOutputStream(path.toFile(), true).getChannel()) {
            channel.truncate(newSize);
        }
    }

    public static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException ignored) {
            // ignored
        }
    }

    public static DataOutputStream toDataOutputStream(OutputStream out) {
        if (out == null) {
            return null;
        }
        if (out instanceof DataOutputStream) {
            return (DataOutputStream) out;
        }
        return new DataOutputStream(out);
    }

    public static <T, R> java.util.function.Function<T, R> unchecked(IOUtils.Function<T, R> ioFunction) {
        return t -> {
            try {
                return ioFunction.apply(t);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static <T> java.util.function.Supplier<T> unchecked(IOUtils.Supplier<T> ioFunction) {
        return () -> {
            try {
                return ioFunction.get();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static <T> java.util.function.Consumer<T> unchecked(IOUtils.Consumer<T> ioFunction) {
        return (t) -> {
            try {
                ioFunction.accept(t);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    @FunctionalInterface
    public interface Supplier<T> {

        /**
         * Gets a result.
         *
         * @throws IOException an exception of its type occurs in the supplier
         * @return a result
         */
        T get() throws IOException;
    }

    @FunctionalInterface
    public interface Function<T, R> {

        R apply(T t) throws IOException;
    }

    @FunctionalInterface
    public interface Consumer<T> {

        void accept(T t) throws IOException;
    }
}
