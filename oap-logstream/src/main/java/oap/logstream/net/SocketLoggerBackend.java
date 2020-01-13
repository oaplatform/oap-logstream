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
import oap.message.MessageSender;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static oap.logstream.AvailabilityReport.State.FAILED;
import static oap.logstream.AvailabilityReport.State.OPERATIONAL;
import static oap.logstream.LogStreamProtocol.MESSAGE_TYPE;

@Slf4j
@ToString
public class SocketLoggerBackend extends LoggerBackend {
    private final MessageSender sender;
    private final Scheduled scheduled;
    private final Counter socketRecv;
    private final Timer bufferSendTime;
    protected int maxBuffers = 5000;
    protected long timeout = 5000;
    protected boolean blocking = true;
    private Buffers buffers;
    private boolean loggingAvailable = true;
    private boolean closed = false;

    public SocketLoggerBackend(MessageSender sender, int bufferSize, long flushIntervar) {
        this(sender, BufferConfigurationMap.DEFAULT(bufferSize), flushIntervar);
    }

    public SocketLoggerBackend(MessageSender sender, BufferConfigurationMap configurations, long flushInterval) {
        this.sender = sender;
        this.buffers = new Buffers(configurations);
        this.scheduled = flushInterval > 0
                ? Scheduler.scheduleWithFixedDelay(flushInterval, TimeUnit.MILLISECONDS, () -> send(true))
                : null;
        configurations.forEach((name, conf) -> Metrics.gauge("logstream_logging_buffers_cache",
                buffers.cache,
                c -> c.size(conf.bufferSize)
        ));
        socketRecv = Metrics.counter("logstream_logging_socket_recv");
        bufferSendTime = Metrics.timer("logstream_logging_buffer_send_time");
    }

    public SocketLoggerBackend(MessageSender sender, int bufferSize) {
        this(sender, BufferConfigurationMap.DEFAULT(bufferSize));
    }

    public SocketLoggerBackend(MessageSender sender, BufferConfigurationMap configurations) {
        this(sender, configurations, 5000);
    }

    public synchronized void send(boolean wait) {
        if (!closed) try {
            if (buffers.isEmpty()) loggingAvailable = true;

            log.debug("sending data to server...");

            buffers.forEachReadyData(b -> {
                try {
                    var completableFuture = sendBuffer(b);
                    completableFuture = completableFuture.thenRun(() -> {
                        loggingAvailable = true;
                    });
                    if (wait)
                        completableFuture.get(timeout, TimeUnit.MILLISECONDS);
                    return true;
                } catch (Exception e) {
                    loggingAvailable = false;
                    return false;
                }
            });

            log.debug("sending done");
        } catch (Exception e) {
            loggingAvailable = false;
            listeners.fireError(new LoggerException(e));
            log.warn(e.getMessage());
            log.trace(e.getMessage(), e);
        }

        if (!loggingAvailable) log.debug("logging unavailable");

    }

    private CompletableFuture<?> sendBuffer(Buffer buffer) {
        return bufferSendTime.record(() -> {
            if (log.isTraceEnabled())
                log.trace("sending {}", buffer);

            return sender.sendObject(MESSAGE_TYPE, buffer.data());
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

        send(false);

        Scheduled.cancel(scheduled);
        Closeables.close(buffers);
    }

    @Override
    public AvailabilityReport availabilityReport() {
        boolean operational = loggingAvailable && !closed && buffers.readyBuffers() < maxBuffers;
        return new AvailabilityReport(operational ? OPERATIONAL : FAILED);
    }
}
