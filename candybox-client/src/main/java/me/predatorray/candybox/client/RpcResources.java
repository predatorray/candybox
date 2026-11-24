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
package me.predatorray.candybox.client;

import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.protocol.ProtocolException;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.Transport;

/** Bounded blocking RPC resources. A cancelled caller owns only its current borrowed connection. */
final class RpcResources implements AutoCloseable {
    private final Transport transport;
    private final int connectionsPerNode;
    private final long acquireMillis;
    private final long requestMillis;
    private final ThreadPoolExecutor executor;
    private final Semaphore admission;
    private final java.util.concurrent.ConcurrentMap<String, Pool> pools = new ConcurrentHashMap<>();
    private final Set<Call> calls = ConcurrentHashMap.newKeySet();
    private final ThreadLocal<Call> current = new ThreadLocal<>();
    private volatile boolean closed;

    RpcResources(Transport transport, int connectionsPerNode, int maxCalls,
                 long acquireMillis, long requestMillis) {
        if (connectionsPerNode < 1 || maxCalls < 1 || acquireMillis < 1 || requestMillis < 1) {
            throw new IllegalArgumentException("RPC limits must be positive");
        }
        this.transport = transport;
        this.connectionsPerNode = connectionsPerNode;
        this.acquireMillis = acquireMillis;
        this.requestMillis = requestMillis;
        admission = new Semaphore(maxCalls);
        // A bounded handoff queue accepts the next call after an action has completed but before
        // its worker has returned to the executor. The permits bound actions including that queue.
        executor = new ThreadPoolExecutor(maxCalls, maxCalls, 30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxCalls), runnable -> {
                    Thread thread = new Thread(runnable, "candybox-client-rpc");
                    thread.setDaemon(true);
                    return thread;
                });
        executor.allowCoreThreadTimeOut(true);
    }

    <T> T call(Callable<T> action) {
        Call call = new Call();
        if (!admission.tryAcquire()) {
            throw new BusyException("Client RPC admission limit reached");
        }
        FutureTask<T> future = new FutureTask<T>(() -> {
            current.set(call);
            try {
                call.check();
                return action.call();
            } finally {
                call.interrupted = Thread.currentThread().isInterrupted();
                current.remove();
                calls.remove(call);
                call.releaseAdmission(); // Before publishing the result to the caller.
            }
        }) {
            @Override
            public void run() {
                try {
                    super.run();
                } finally {
                    // Cancellation before execution skips the callable and its finally block.
                    call.releaseAdmission();
                }
            }
        };
        call.future = future;
        calls.add(call);
        if (closed) {
            calls.remove(call);
            call.releaseAdmission();
            throw new ProtocolException("Client router is closed");
        }
        try {
            executor.execute(future);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            calls.remove(call);
            call.releaseAdmission();
            throw new BusyException("Client RPC admission limit reached or router closed");
        }
        try {
            return future.get(Math.max(1, call.deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            call.cancel();
            throw timeout("Overall RPC deadline expired; a dispatched mutation may have applied");
        } catch (InterruptedException e) {
            call.cancel();
            Thread.currentThread().interrupt();
            throw new ProtocolException("Interrupted waiting for RPC; outcome may be unknown", e);
        } catch (java.util.concurrent.CancellationException e) {
            throw new ProtocolException("Client router closed or RPC cancelled", e);
        } catch (ExecutionException e) {
            if (call.interrupted) { Thread.currentThread().interrupt(); }
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (e.getCause() instanceof Error error) {
                throw error;
            }
            throw new ProtocolException("RPC failed", e.getCause());
        } finally {
            // A task cancelled before its worker starts never enters the callable's finally.
            calls.remove(call);
        }
    }

    <T> T send(String host, int port, java.util.function.Function<Connection, T> action) {
        Call call = current.get();
        call.check();
        Pool pool = pools.computeIfAbsent(host + ":" + port, ignored -> new Pool());
        Connection connection = pool.borrow(host, port, call);
        boolean healthy = false;
        try {
            call.attach(connection);
            T result = action.apply(connection);
            call.check();
            healthy = true;
            return result;
        } finally {
            call.detach(connection);
            pool.release(connection, healthy && !call.cancelled);
        }
    }

    static final class SetupFailure extends RuntimeException {
        SetupFailure(RuntimeException cause) { super(cause); }
        RuntimeException failure() { return (RuntimeException) getCause(); }
    }

    private final class Pool {
        private final ArrayDeque<Connection> idle = new ArrayDeque<>();
        private final Set<Connection> all = Collections.newSetFromMap(new IdentityHashMap<>());
        private int opening;

        Connection borrow(String host, int port, Call call) {
            long until = Math.min(call.deadline,
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(acquireMillis));
            synchronized (this) {
                while (true) {
                    call.check();
                    if (!idle.isEmpty()) {
                        return idle.removeFirst();
                    }
                    if (all.size() + opening < connectionsPerNode) {
                        opening++;
                        break;
                    }
                    long remaining = until - System.nanoTime();
                    if (remaining <= 0) {
                        throw timeout("Connection acquisition deadline expired");
                    }
                    try {
                        TimeUnit.NANOSECONDS.timedWait(this, remaining);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new ProtocolException("Interrupted acquiring connection", e);
                    }
                }
            }
            Connection fresh;
            try {
                fresh = transport.connect(host, port);
            } catch (RuntimeException e) {
                synchronized (this) {
                    opening--;
                    notifyAll();
                }
                throw new SetupFailure(e);
            }
            synchronized (this) {
                opening--;
                if (closed || call.cancelled) {
                    closeQuietly(fresh);
                    notifyAll();
                    throw new ProtocolException("Connection setup cancelled");
                }
                all.add(fresh);
                return fresh;
            }
        }

        synchronized void release(Connection connection, boolean healthy) {
            if (!closed && healthy && all.contains(connection)) {
                idle.addLast(connection);
            } else {
                all.remove(connection);
                closeQuietly(connection);
            }
            notifyAll();
        }

        synchronized void shutdown() {
            all.forEach(RpcResources::closeQuietly);
            all.clear();
            idle.clear();
            notifyAll();
        }
    }

    private final class Call {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(requestMillis);
        volatile boolean cancelled;
        volatile boolean interrupted;
        FutureTask<?> future;
        private Connection borrowed;
        private final AtomicBoolean admissionReleased = new AtomicBoolean();

        void releaseAdmission() {
            if (admissionReleased.compareAndSet(false, true)) {
                admission.release();
            }
        }

        void check() {
            if (closed || cancelled || System.nanoTime() >= deadline) {
                throw timeout("RPC cancelled or deadline expired");
            }
        }

        synchronized void attach(Connection connection) {
            check();
            borrowed = connection;
        }

        synchronized void detach(Connection connection) {
            if (borrowed == connection) {
                borrowed = null;
            }
        }

        synchronized void cancel() {
            cancelled = true;
            if (borrowed != null) {
                closeQuietly(borrowed);
            }
            future.cancel(true);
        }
    }

    private static ProtocolException timeout(String message) {
        return new ProtocolException(message, new SocketTimeoutException(message));
    }

    private static void closeQuietly(Connection connection) {
        try {
            connection.close();
        } catch (RuntimeException ignored) {
            // The connection is discarded even if close fails.
        }
    }

    @Override
    public void close() {
        closed = true;
        calls.forEach(Call::cancel);
        pools.values().forEach(Pool::shutdown);
        executor.shutdownNow();
    }
}
