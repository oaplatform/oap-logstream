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
import oap.message.MessageAvailabilityReport;
import oap.message.MessageSender;
import oap.message.MessageStatus;
import oap.util.Dates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static oap.logstream.AvailabilityReport.State.FAILED;
import static oap.logstream.AvailabilityReport.State.OPERATIONAL;
import static oap.logstream.LogStreamProtocol.MESSAGE_TYPE;
import static oap.logstream.LogStreamProtocol.STATUS_BACKEND_LOGGER_NOT_AVAILABLE;

@Slf4j
@ToString
public class SocketLoggerBackend extends LoggerBackend {
    public static final String FAILURE_IO_STATE = "IO";
    public static final String FAILURE_BUFFERS_STATE = "BUFFERS";
    public static final String FAILURE_SHUTDOWN_STATE = "SHUTDOWN";

    private final MessageSender sender;
    private final Scheduled scheduled;
    private final Timer bufferSendTime = Metrics.timer( "logstream_logging_buffer_send_time" );
    private final Counter logstreamSendSuccess = Metrics.counter( "logstream_send", "status", "success" );
    private final Counter logstreamSendTimeout = Metrics.counter( "logstream_send", "status", "timeout" );
    private final Counter logstreamSendError = Metrics.counter( "logstream_send", "status", "error" );
    private final Buffers buffers;
    public int maxBuffers = 5000;
    public long timeout = Dates.h( 1 );
    private boolean closed = false;

    public SocketLoggerBackend( MessageSender sender, int bufferSize, long flushInterval ) {
        this( sender, BufferConfigurationMap.DEFAULT( bufferSize ), flushInterval );
    }

    public SocketLoggerBackend( MessageSender sender, BufferConfigurationMap configurations, long flushInterval ) {
        this.sender = sender;
        this.buffers = new Buffers( configurations );
        this.scheduled = flushInterval > 0
            ? Scheduler.scheduleWithFixedDelay( flushInterval, TimeUnit.MILLISECONDS, this::send )
            : null;
        configurations.forEach( ( name, conf ) -> Metrics.gauge( "logstream_logging_buffers_cache",
            buffers.cache,
            c -> c.size( conf.bufferSize )
        ) );
    }

    public SocketLoggerBackend( MessageSender sender, int bufferSize ) {
        this( sender, BufferConfigurationMap.DEFAULT( bufferSize ) );
    }

    public SocketLoggerBackend( MessageSender sender, BufferConfigurationMap configurations ) {
        this( sender, configurations, 5000 );
    }

    public synchronized boolean send() {
        return send( false );
    }

    public synchronized boolean send( boolean force ) {
        if( force || !closed ) {
            var start = System.nanoTime();
            try {
                log.debug( "sending data to server..." );

                var res = new ArrayList<CompletableFuture<MessageStatus>>();

                buffers.forEachReadyData( b -> {
                    try {
                        if( log.isTraceEnabled() )
                            log.trace( "sending {}", b );

                        res.add( sender.sendObject( MESSAGE_TYPE, b.data(),
                            status -> status == STATUS_BACKEND_LOGGER_NOT_AVAILABLE ? "BACKEND_LOGGER_NOT_AVAILABLE"
                                : null ) );

                        return true;
                    } catch( Exception e ) {
                        if( log.isTraceEnabled() )
                            log.trace( e.getMessage(), e );
                        else log.debug( "SEND ERROR: {}", e.getMessage() );

                        return false;
                    }
                } );

                CompletableFuture
                    .allOf( res.toArray( new CompletableFuture[0] ) )
                    .get( timeout, TimeUnit.MILLISECONDS );

                log.debug( "sending done" );
                logstreamSendSuccess.increment();
                return true;
            } catch( TimeoutException e ) {
                logstreamSendTimeout.increment();
                return false;
            } catch( Exception e ) {
                logstreamSendError.increment();
                listeners.fireError( new LoggerException( e ) );
                log.debug( e.getMessage(), e );
                return false;
            } finally {
                bufferSendTime.record( System.nanoTime() - start, TimeUnit.NANOSECONDS );
            }
        }

        return false;
    }

    @Override
    public void log( String hostName, String filePreffix, Map<String, String> properties, String logType,
                     int shard, String headers, byte[] buffer, int offset, int length ) {
        buffers.put( new LogId( filePreffix, logType, hostName, shard, properties, headers ), buffer, offset, length );
    }

    @Override
    public synchronized void close() {
        closed = true;

        send( true );

        Scheduled.cancel( scheduled );
        Closeables.close( buffers );
    }

    @Override
    public AvailabilityReport availabilityReport() {
        var ioFailed = sender.availabilityReport().state != MessageAvailabilityReport.State.OPERATIONAL;
        var buffersFailed = this.buffers.readyBuffers() >= maxBuffers;
        var operational = /*!ioFailed && */!closed && !buffersFailed;
        if( !operational ) {
            var state = new HashMap<String, AvailabilityReport.State>();
            state.put( FAILURE_IO_STATE, ioFailed ? FAILED : OPERATIONAL );
            state.put( FAILURE_BUFFERS_STATE, buffersFailed ? FAILED : OPERATIONAL );
            state.put( FAILURE_SHUTDOWN_STATE, closed ? FAILED : OPERATIONAL );

            if( buffersFailed ) this.buffers.report();

            return new AvailabilityReport( FAILED, state );
        } else
            return new AvailabilityReport( OPERATIONAL );
    }
}
