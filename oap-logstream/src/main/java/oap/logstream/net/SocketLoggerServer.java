/*
 * The MIT License (MIT)
 *
 * Copyright (c) Open Application Platform Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package oap.logstream.net;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import oap.concurrent.SynchronizedThread;
import oap.concurrent.ThreadPoolExecutor;
import oap.io.Closeables;
import oap.io.Files;
import oap.io.Sockets;
import oap.logstream.BackendLoggerNotAvailableException;
import oap.logstream.BufferOverflowException;
import oap.logstream.LoggerBackend;
import oap.logstream.LoggerException;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static oap.concurrent.Threads.isInterrupted;

@Slf4j
public class SocketLoggerServer extends SocketServer {

    private final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(0, 1024, 100, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    new ThreadFactoryBuilder().setNameFormat("socket-logging-worker-%d").build());
    private final SynchronizedThread thread = new SynchronizedThread(this);

    protected int soTimeout = 60000;
    private int port;
    private int bufferSize;
    private LoggerBackend backend;
    private Path controlStatePath;
    private ServerSocket serverSocket;
    private ConcurrentHashMap<String, AtomicLong> control = new ConcurrentHashMap<>();

    public SocketLoggerServer(int port, int bufferSize, LoggerBackend backend, Path controlStatePath) {
        this.port = port;
        this.bufferSize = bufferSize;
        this.backend = backend;
        this.controlStatePath = controlStatePath;
        Metrics.gauge("logstream_logging_server_workers", List.of(Tag.of("port", String.valueOf(port))),
                executor, e -> e.getTaskCount() - e.getCompletedTaskCount());
    }

    @Override
    public void run() {
        try {
            while (thread.isRunning() && !serverSocket.isClosed()) try {
                Socket socket = serverSocket.accept();
                log.debug("accepted connection {}", socket);
                executor.execute(new LogSocketHandler(socket));
            } catch (SocketTimeoutException ignore) {
            } catch (IOException e) {
                if (!"Socket closed".equals(e.getMessage()))
                    log.error(e.getMessage(), e);
            }
        } finally {
            Closeables.close(serverSocket);
            Closeables.close(executor);
        }
    }

    public void start() {
        try {
            if (controlStatePath.toFile().exists()) this.control = Files.readObject(controlStatePath);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));
            log.debug("ready to rock " + serverSocket.getLocalSocketAddress());
            thread.start();

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void stop() {
        Closeables.close(serverSocket);
        thread.stop();
        Closeables.close(executor);
        Files.writeObject(controlStatePath, control);
    }

    public class LogSocketHandler implements Runnable, Closeable {
        private Socket socket;
        private byte[] buffer = new byte[bufferSize];
        private boolean closed;

        public LogSocketHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            String hostName = null;
            byte clientId = -1;

            try {
                var out = new DataOutputStream(socket.getOutputStream());
                var in = new DataInputStream(socket.getInputStream());
                socket.setSoTimeout(soTimeout);
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);

                hostName = socket.getInetAddress().getCanonicalHostName();
                clientId = in.readByte();
                var digestKey = hostName + clientId;

                log.info("client = {}/{}", hostName, clientId);

                log.debug("[{}/{}] start logging... ", hostName, clientId);
                while (!closed && !isInterrupted()) {
                    long digestionId = in.readLong();
                    var lastId = control.computeIfAbsent(digestKey, h -> new AtomicLong(0L));
                    int size = in.readInt();
                    String logName = in.readUTF();
                    String logType = in.readUTF();
                    String clientHostname = in.readUTF();
                    int shard = in.readInt();
                    var headers = in.readUTF();
                    if (size > bufferSize) {
                        out.writeInt(SocketError.BUFFER_OVERFLOW.code);
                        var exception = new BufferOverflowException(hostName, clientId, logName, logType, headers, bufferSize, size);
                        backend.listeners.fireError(exception);
                        throw exception;
                    }
                    in.readFully(buffer, 0, size);
                    if (!backend.isLoggingAvailable()) {
                        out.writeInt(SocketError.BACKEND_UNAVAILABLE.code);
                        var exception = new BackendLoggerNotAvailableException(hostName, clientId);
                        backend.listeners.fireError(exception);
                        throw exception;
                    }
                    if (lastId.get() < digestionId) {
                        log.trace("[{}/{}] logging ({}, {}/{}/{}, {})", hostName, clientId, digestionId, logName, logType, headers, size);
                        backend.log(clientHostname, logName, logType, shard, headers, buffer, 0, size);
                        lastId.set(digestionId);
                    } else {
                        var message = "[" + hostName + "/" + clientId + "] buffer (" + digestionId + ", " + logName + "/" + logType + "/" + headers
                                + ", " + size + ") already written. Last written buffer is (" + lastId + ")";
                        log.warn(message);
                        backend.listeners.fireWarning(message);
                    }
                    out.writeInt(size);
                }
            } catch (EOFException e) {
                var msg = "[" + hostName + "/" + clientId + "] " + socket + " ended, closed";
                backend.listeners.fireWarning(msg);
                log.debug(msg);
            } catch (SocketTimeoutException e) {
                var msg = "[" + hostName + "/" + clientId + "] no activity on socket for " + soTimeout + "ms, timeout, closing...";
                backend.listeners.fireWarning(msg);
                log.info(msg);
                log.trace("[" + hostName + "/" + clientId + "] " + e.getMessage(), e);
            } catch (LoggerException e) {
                log.error("[" + hostName + "/" + clientId + "] " + e.getMessage(), e);
            } catch (Exception e) {
                backend.listeners.fireWarning("[" + hostName + "/" + clientId + "] ");
                log.error("[" + hostName + "/" + clientId + "] " + e.getMessage(), e);
            } finally {
                Sockets.close(socket);
            }
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}
