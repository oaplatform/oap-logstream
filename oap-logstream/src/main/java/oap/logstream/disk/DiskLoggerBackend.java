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
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.concurrent.Executors;
import oap.concurrent.scheduler.ScheduledExecutorService;
import oap.google.JodaTicker;
import oap.io.Closeables;
import oap.io.Files;
import oap.logstream.AbstractLoggerBackend;
import oap.logstream.AvailabilityReport;
import oap.logstream.LogId;
import oap.logstream.LogStreamProtocol.ProtocolVersion;
import oap.logstream.LoggerException;
import oap.logstream.Timestamp;
import oap.util.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oap.logstream.AvailabilityReport.State.FAILED;
import static oap.logstream.AvailabilityReport.State.OPERATIONAL;

@Slf4j
public class DiskLoggerBackend extends AbstractLoggerBackend implements Cloneable, AutoCloseable {
    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor
    public static class FilePatternConfiguration {
        public final String path;
        public final List<LogFormat> formats;
    }

    public static final int DEFAULT_BUFFER = 1024 * 100;
    public static final long DEFAULT_FREE_SPACE_REQUIRED = 2000000000L;
    private final Path logDirectory;
    private final Timestamp timestamp;
    private final int bufferSize;
    private final LoadingCache<LogId, List<AbstractWriter<? extends Closeable>>> writers;
    private final ScheduledExecutorService pool;
    public String filePattern = "/<YEAR>-<MONTH>/<DAY>/<LOG_TYPE>_v<LOG_VERSION>_<CLIENT_HOST>-<YEAR>-<MONTH>-<DAY>-<HOUR>-<INTERVAL>.${LOG_FORMAT}";
    public final List<LogFormat> formats;
    public final LinkedHashMap<String, FilePatternConfiguration> filePatternByType = new LinkedHashMap<>();
    public long requiredFreeSpace = DEFAULT_FREE_SPACE_REQUIRED;
    public int maxVersions = 20;
    private volatile boolean closed;

    public final WriterConfiguration writerConfiguration = new WriterConfiguration();

    @SuppressWarnings( "unchecked" )
    public DiskLoggerBackend( Path logDirectory, List<LogFormat> formats, Timestamp timestamp, int bufferSize ) {
        log.info( "logDirectory '{}' formats {} timestamp {} bufferSize {} writerConfiguration {}",
            logDirectory, formats, timestamp, FileUtils.byteCountToDisplaySize( bufferSize ), writerConfiguration );

        Preconditions.checkArgument( !formats.isEmpty() );

        this.logDirectory = logDirectory;
        this.formats = formats;
        this.timestamp = timestamp;
        this.bufferSize = bufferSize;

        this.writers = CacheBuilder.newBuilder()
            .ticker( JodaTicker.JODA_TICKER )
            .expireAfterAccess( 60 / timestamp.bucketsPerHour * 3, TimeUnit.MINUTES )
            .removalListener( notification -> {
                for( var closeable : ( List<? extends Closeable> ) notification.getValue() ) {
                    Closeables.close( closeable );
                }
            } )
            .build( new CacheLoader<>() {
                @NotNull
                @Override
                public List<AbstractWriter<? extends Closeable>> load( @NotNull LogId id ) {
                    List<AbstractWriter<? extends Closeable>> writers = new ArrayList<>();

                    var fp = filePatternByType.getOrDefault( id.logType, new FilePatternConfiguration( filePattern, formats ) );

                    for( var format : fp.formats ) {
                        log.trace( "new writer id '{}' filePattern '{}'", id, fp );

                        switch( format ) {
                            case PARQUET -> writers.add( new ParquetWriter( logDirectory, fp.path, id,
                                writerConfiguration.parquet, bufferSize, timestamp, maxVersions ) );
                            case TSV_GZ -> writers.add( new TsvWriter( logDirectory, fp.path, id,
                                writerConfiguration.tsv, bufferSize, timestamp, maxVersions ) );
                            default -> throw new IllegalArgumentException( "Unsupported encoding " + format );
                        }
                    }

                    return writers;
                }
            } );
        Metrics.gauge( "logstream_logging_disk_writers", List.of( Tag.of( "path", logDirectory.toString() ) ),
            writers, Cache::size );

        pool = Executors.newScheduledThreadPool( 1, "disk-logger-backend" );
        pool.scheduleWithFixedDelay( () -> refresh( false ), 10, 10, SECONDS );
    }

    @Override
    @SneakyThrows
    public void log( ProtocolVersion protocolVersion, String hostName, String filePreffix, Map<String, String> properties, String logType, int shard,
                     String[] headers, byte[][] types, byte[] buffer, int offset, int length ) {
        if( closed ) {
            var exception = new LoggerException( "already closed!" );
            listeners.fireError( exception );
            throw exception;
        }

        Metrics.counter( "logstream_logging_disk_counter", List.of( Tag.of( "from", hostName ) ) ).increment();
        Metrics.summary( "logstream_logging_disk_buffers", List.of( Tag.of( "from", hostName ) ) ).record( length );
        List<AbstractWriter<? extends Closeable>> writerList = writers.get( new LogId( filePreffix, logType, hostName, shard, properties, headers, types ) );
        for( int iw = 0; iw < writerList.size(); iw++ ) {
            AbstractWriter<? extends Closeable> writer = writerList.get( iw );

            log.trace( "logging {} bytes to {}", length, writer );
            try {
                writer.write( protocolVersion, buffer, offset, length, this.listeners::fireError );
            } catch( Exception e ) {
                var headersWithTypes = new ArrayList<String>();
                for( int i = 0; i < headers.length; i++ ) {
                    headersWithTypes.add( headers[i] + " [" + Lists.map( List.of( ArrayUtils.toObject( types[i] ) ), oap.template.Types::valueOf ) + "]" );
                }

                log.error( "hostName {} filePrefix {} logType {} properties {} shard {} headers {} path {}",
                    hostName, filePreffix, logType, properties, shard, headersWithTypes, writer.currentPattern() );
                boolean isThereAlreadyRecordedLog = iw > 0;
                if( !isThereAlreadyRecordedLog ) {
                    throw e;
                }
            }
        }
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
        long usableSpaceAtDirectory = Files.usableSpaceAtDirectory( logDirectory );
        var enoughSpace = usableSpaceAtDirectory > requiredFreeSpace;
        if( !enoughSpace ) {
            log.error( "There is no enough space on device {}, required {}, but {} available", logDirectory, requiredFreeSpace, usableSpaceAtDirectory );
        }
        return new AvailabilityReport( enoughSpace ? OPERATIONAL : FAILED );
    }

    public void refresh() {
        refresh( false );
    }

    public void refresh( boolean forceSync ) {
        for( var writerList : writers.asMap().values() ) {
            for( var writer : writerList ) {
                try {
                    writer.refresh( forceSync );
                } catch( Exception e ) {
                    log.error( "Cannot refresh ", e );
                }
            }
        }

        writers.cleanUp();
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
