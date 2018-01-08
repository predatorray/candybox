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

package me.predatorray.candybox.discovery;

import java.io.IOException;

/**
 * Abstract base class for Discovery implementations, providing common functions.
 *
 * @author Wenhao Ji
 */
public abstract class AbstractDiscovery implements Discovery {

    private volatile boolean closed = false;

    /**
     * Close the instance, mark the instance as closed.
     * <p>Implementations that override this method should call {@code super.close()} before other closing steps.</p>
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        closed = true;
    }

    /**
     * Ensure this discovery instance is not closed, and throw @{code IllegalStateException} if it .
     * <p>This method may be used by implementations at the beginning of each method
     * that only works when the instance is open. </p>
     *
     * @throws IllegalStateException if the instance has already been closed
     */
    protected void ensureNotClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("The discovery has been already closed");
        }
    }
}
