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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static oap.logstream.AvailabilityReport.State.FAILED;
import static oap.logstream.AvailabilityReport.State.OPERATIONAL;
import static oap.logstream.LogStreamProtocol.MESSAGE_TYPE;

@Slf4j
@ToString
public class SocketLoggerBackend extends LoggerBackend {
    public static final String FAILURE_IO_STATE = "IO";
    public static final String FAILURE_BUFFERS_STATE = "BUFFERS";
    public static final String FAILURE_SHUTDOWN_STATE = "SHUTDOWN";

    private final MessageSender sender;
    private final Scheduled scheduled;
    private final Timer bufferSendTime;
    protected int maxBuffers = 5000;
    protected long timeout = 5000;
    protected boolean blocking = true;
    private Buffers buffers;
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
        bufferSendTime = Metrics.timer( "logstream_logging_buffer_send_time" );
    }

    public SocketLoggerBackend( MessageSender sender, int bufferSize ) {
        this( sender, BufferConfigurationMap.DEFAULT( bufferSize ) );
    }

    public SocketLoggerBackend( MessageSender sender, BufferConfigurationMap configurations ) {
        this( sender, configurations, 5000 );
    }

    public synchronized void send() {
        if( !closed ) try {
            log.debug( "sending data to server..." );

            var res = new ArrayList<CompletableFuture<?>>();

            buffers.forEachReadyData( b -> {
                try {
                    res.add( sendBuffer( b ) );

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
                .get();

            log.debug( "sending done" );
        } catch( Exception e ) {
            listeners.fireError( new LoggerException( e ) );
            log.warn( e.getMessage() );
            log.trace( e.getMessage(), e );
        }
    }

    private CompletableFuture<?> sendBuffer( Buffer buffer ) {
        return bufferSendTime.record( () -> {
            if( log.isTraceEnabled() )
                log.trace( "sending {}", buffer );

            return sender.sendObject( MESSAGE_TYPE, buffer.data() );
        } );
    }

    @Override
    public void log( String hostName, String filePreffix, Map<String, String> properties, String logType,
                     int shard, String headers, byte[] buffer, int offset, int length ) {
        buffers.put( new LogId( filePreffix, logType, hostName, shard, properties, headers ), buffer, offset, length );
    }

    @Override
    public synchronized void close() {
        closed = true;

        Scheduled.cancel( scheduled );
        Closeables.close( buffers );
    }

    @Override
    public AvailabilityReport availabilityReport() {
        var io = sender.availabilityReport().state != MessageAvailabilityReport.State.OPERATIONAL;
        var buffers = this.buffers.readyBuffers() >= maxBuffers;
        var operational = /*!io && */!closed && !buffers;
        if( !operational ) {
            var state = new HashMap<String, AvailabilityReport.State>();
            state.put( FAILURE_IO_STATE, !io ? OPERATIONAL : FAILED );
            state.put( FAILURE_BUFFERS_STATE, !buffers ? OPERATIONAL : FAILED );
            state.put( FAILURE_SHUTDOWN_STATE, !closed ? OPERATIONAL : FAILED );

            if( !buffers ) this.buffers.report();

            return new AvailabilityReport( FAILED, state );
        } else
            return new AvailabilityReport( OPERATIONAL );
    }
}
