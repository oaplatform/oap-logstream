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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.concurrent.scheduler.Scheduled;
import oap.concurrent.scheduler.Scheduler;
import oap.io.Closeables;
import oap.logstream.AvailabilityReport;
import oap.logstream.LogId;
import oap.logstream.LoggerBackend;
import oap.logstream.LoggerException;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static oap.logstream.AvailabilityReport.State.FAILED;
import static oap.logstream.AvailabilityReport.State.OPERATIONAL;

@Slf4j
@ToString(of = {"host"})
public class SocketLoggerBackend extends LoggerBackend {

    private final byte clientId;
    private final String host;
    private final int port;
    private final Scheduled scheduled;
    private final Counter socketRecv;
    private final Timer bufferSendTime;
    protected int maxBuffers = 5000;
    protected long timeout = 5000;
    protected boolean blocking = true;
    private Connection connection;
    private Buffers buffers;
    private boolean loggingAvailable = true;
    private boolean closed = false;

    public SocketLoggerBackend(byte clientId, String host, int port, Path location, int bufferSize, long flushIntervar) {
        this(clientId, host, port, location, BufferConfigurationMap.DEFAULT(bufferSize), flushIntervar);
    }

    public SocketLoggerBackend(byte clientId, String host, int port, Path location, BufferConfigurationMap configurations, long flushInterval) {
        this.clientId = clientId;
        this.host = host;
        this.port = port;
        this.buffers = new Buffers(location, configurations);
        this.scheduled = Scheduler.scheduleWithFixedDelay(flushInterval, TimeUnit.MILLISECONDS, this::send);
        configurations.forEach((name, conf) -> Metrics.gauge("logstream_logging_buffers_cache",
                buffers.cache,
                c -> c.size(conf.bufferSize)
        ));
        socketRecv = Metrics.counter("logstream_logging_socket_recv", "from_host", host);
        bufferSendTime = Metrics.timer("logstream_logging_buffer_send_time", "from_host", host);
    }

    public SocketLoggerBackend(byte clientId, String host, int port, Path location, int bufferSize) {
        this(clientId, host, port, location, BufferConfigurationMap.DEFAULT(bufferSize));
    }

    public SocketLoggerBackend(byte clientId, String host, int port, Path location, BufferConfigurationMap configurations) {
        this(clientId, host, port, location, configurations, 5000);
    }

    public synchronized void send() {
        if (!closed) try {
            if (buffers.isEmpty()) loggingAvailable = true;

            refreshConnection();

            log.debug("sending data to server...");

            buffers.forEachReadyData(buffer -> {
                if (!sendBuffer(buffer)) {
                    log.debug("send unsuccessful...");
                    refreshConnection();
                    return sendBuffer(buffer);
                }
                return true;

            });
            log.debug("sending done");
        } catch (Exception e) {
            loggingAvailable = false;
            listeners.fireError(new LoggerException(e));
            log.warn(e.getMessage());
            log.trace(e.getMessage(), e);
            Closeables.close(connection);
        }

        if (!loggingAvailable) log.debug("logging unavailable");

    }

    private void refreshConnection() {
        if (this.connection == null || !connection.isConnected()) {
            Closeables.close(connection);
            log.debug("opening connection...");
            this.connection =
                    blocking ? new SocketConnection(host, port, timeout) : new ChannelConnection(host, port, timeout);
            connection.write(clientId);
            log.debug("connected!");
        }
    }

    private Boolean sendBuffer(Buffer buffer) {
        return bufferSendTime.record(() -> {
            try {
                if (log.isTraceEnabled())
                    log.trace("sending {}", buffer);
                connection.write(buffer.data(), 0, buffer.length());
                int size = connection.read();
                if (size <= 0) {
                    loggingAvailable = false;
                    var msg = "Error completing remote write: " + SocketError.fromCode(size);
                    listeners.fireError(msg);
                    log.error(msg);
                    return false;
                }
                socketRecv.increment(buffer.length());
                loggingAvailable = true;
                return true;
            } catch (Exception e) {
                loggingAvailable = false;
                log.warn(e.getMessage());
                log.trace(e.getMessage(), e);
                listeners.fireError(e);
                Closeables.close(connection);
                return false;
            }
        });
    }

    @Override
    public void log(String hostName, String filePreffix, Map<String, String> properties, String logType, 
                    int shard, String headers, byte[] buffer, int offset, int length) {
        buffers.put(new LogId(filePreffix, logType, hostName, shard, properties, headers), buffer, offset, length);
    }

    @Override
    public synchronized void close() {
        closed = true;

        send();

        Scheduled.cancel(scheduled);
        Closeables.close(connection);
        Closeables.close(buffers);
    }

    @Override
    public AvailabilityReport availabilityReport() {
        boolean operational = loggingAvailable && !closed && buffers.readyBuffers() < maxBuffers;
        return new AvailabilityReport(operational ? OPERATIONAL : FAILED);
    }
}
