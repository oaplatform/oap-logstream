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

package oap.logstream.disk;

import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import oap.concurrent.Executors;
import oap.concurrent.scheduler.ScheduledExecutorService;
import oap.io.Closeables;
import oap.io.Files;
import oap.logstream.AvailabilityReport;
import oap.logstream.LogId;
import oap.logstream.AbstractLoggerBackend;
import oap.logstream.LoggerException;
import oap.logstream.Timestamp;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oap.logstream.AvailabilityReport.State.FAILED;
import static oap.logstream.AvailabilityReport.State.OPERATIONAL;

@Slf4j
public class DiskLoggerBackend extends AbstractLoggerBackend {
    public static final int DEFAULT_BUFFER = 1024 * 100;
    public static final long DEFAULT_FREE_SPACE_REQUIRED = 2000000000L;
    private final Path logDirectory;
    private final Timestamp timestamp;
    private final int bufferSize;
    private final LoadingCache<LogId, Writer> writers;
    private final ScheduledExecutorService pool;
    public String filePattern = "/${YEAR}-${MONTH}/${DAY}/${LOG_TYPE}_v${LOG_VERSION}_${CLIENT_HOST}-${YEAR}-${MONTH}-${DAY}-${HOUR}-${INTERVAL}.tsv.gz";
    public long requiredFreeSpace = DEFAULT_FREE_SPACE_REQUIRED;
    private boolean closed;

    public DiskLoggerBackend( Path logDirectory, Timestamp timestamp, int bufferSize, boolean withHeaders ) {
        this.logDirectory = logDirectory;
        this.timestamp = timestamp;
        this.bufferSize = bufferSize;
        this.writers = CacheBuilder.newBuilder()
            .expireAfterAccess( 60 / timestamp.bucketsPerHour * 3, TimeUnit.MINUTES )
            .removalListener( notification -> Closeables.close( ( Writer ) notification.getValue() ) )
            .build( new CacheLoader<>() {
                @Override
                public Writer load( LogId id ) {
                    return new Writer( logDirectory, filePattern, id, bufferSize, timestamp, withHeaders );
                }
            } );
        Metrics.gauge( "logstream_logging_disk_writers", List.of( Tag.of( "path", logDirectory.toString() ) ),
            writers, Cache::size );

        pool = Executors.newScheduledThreadPool( 1, "disk-logger-backend" );
        pool.scheduleWithFixedDelay( this::refresh, 10, 10, SECONDS );
    }

    public DiskLoggerBackend( Path logDirectory, Timestamp timestamp, int bufferSize ) {
        this( logDirectory, timestamp, bufferSize, true );
    }

    @Override
    @SneakyThrows
    public void log( String hostName, String filePreffix, Map<String, String> properties, String logType,
                     int shard, String headers, byte[] buffer, int offset, int length ) {
        if( closed ) {
            var exception = new LoggerException( "already closed!" );
            listeners.fireError( exception );
            throw exception;
        }

        Metrics.counter( "logstream_logging_disk_counter", List.of( Tag.of( "from", hostName ) ) ).increment();
        Metrics.summary( "logstream_logging_disk_buffers", List.of( Tag.of( "from", hostName ) ) ).record( length );
        var writer = writers.get( new LogId( filePreffix, logType, hostName, shard, properties, headers ) );
        log.trace( "logging {} bytes to {}", length, writer );
        writer.write( buffer, offset, length, this.listeners::fireError );
    }

    @Override
    public void close() {
        if( !closed ) {
            closed = true;
            pool.shutdown( 20, SECONDS );
            Closeables.close( pool );
            writers.invalidateAll();
        }
    }

    @Override
    public AvailabilityReport availabilityReport() {
        var enoughSpace = Files.usableSpaceAtDirectory( logDirectory ) > requiredFreeSpace;
        return new AvailabilityReport( enoughSpace ? OPERATIONAL : FAILED );
    }

    public void refresh() {
        for( var writer : writers.asMap().values() ) {
            try {
                writer.refresh();
            } catch( Exception e ) {
                log.error( e.getMessage(), e );
            }
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper( this )
            .add( "path", logDirectory )
            .add( "filePattern", filePattern )
            .add( "buffer", bufferSize )
            .add( "bucketsPerHour", timestamp.bucketsPerHour )
            .add( "writers", writers.size() )
            .toString();
    }
}
