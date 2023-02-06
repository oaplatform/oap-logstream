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

import com.google.common.base.Preconditions;
import com.google.common.io.CountingOutputStream;
import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;
import oap.concurrent.Stopwatch;
import oap.io.IoStreams;
import oap.io.IoStreams.Encoding;
import oap.logstream.LogId;
import oap.logstream.LoggerException;
import oap.logstream.Timestamp;
import oap.util.Dates;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings( "UnstableApiUsage" )
@Slf4j
public class Writer implements Closeable, AutoCloseable {
    public static final int MAX_VERSION = 50;
    private final Path logDirectory;
    private final String filePattern;
    private final LogId logId;
    private final Timestamp timestamp;
    private final int bufferSize;
    private CountingOutputStream out;
    private String lastPattern;
    private final Stopwatch stopwatch = new Stopwatch();
    private int version = 1;
    private final boolean withHeaders;
    private boolean closed = false;

    public Writer( Path logDirectory, String filePattern, LogId logId, int bufferSize, Timestamp timestamp, boolean withHeaders ) {
        this.logDirectory = logDirectory;
        this.filePattern = filePattern;

        Preconditions.checkArgument( filePattern.contains( "${LOG_VERSION}" ) );

        this.logId = logId;
        this.bufferSize = bufferSize;
        this.timestamp = timestamp;
        this.lastPattern = currentPattern();
        this.withHeaders = withHeaders;
        log.debug( "spawning {}", this );
    }

    public Writer( Path logDirectory, String filePattern, LogId logId, int bufferSize, Timestamp timestamp ) {
        this( logDirectory, filePattern, logId, bufferSize, timestamp, true );
    }

    @Override
    public synchronized void close() {
        log.debug( "closing {}", this );
        closed = true;
        closeOutput();
    }

    private void closeOutput() throws LoggerException {
        if( out != null ) try {
            log.trace( "closing output {} ({} bytes)", this, out.getCount() );
            stopwatch.count( out::flush );
            stopwatch.count( out::close );
        } finally {
            Metrics.summary( "logstream_logging_server_bucket_size" ).record( out.getCount() );
            Metrics.summary( "logstream_logging_server_bucket_time_seconds" ).record( Dates.nanosToSeconds( stopwatch.elapsed() ) );
            out = null;
        }
    }

    public synchronized void write( byte[] buffer, Consumer<String> error ) throws LoggerException {
        write( buffer, 0, buffer.length, error );
    }

    public synchronized void write( byte[] buffer, int offset, int length, Consumer<String> error ) throws LoggerException {
        if( closed ) {
            throw new LoggerException( "writer is already closed!" );
        }
        Path filename = null;
        try {
            refresh( false );
            filename = filename();
            if( out == null )
                if( !java.nio.file.Files.exists( filename ) ) {
                    log.info( "[{}] open new file v{}", filename, version );
                    out = new CountingOutputStream( IoStreams.out( filename, Encoding.from( filename ), bufferSize ) );
                    new LogMetadata( logId ).withProperty( "VERSION", logId.getHashWithVersion( version ) ).writeFor( filename );
                    if( withHeaders ) {
                        out.write( logId.headers.getBytes( UTF_8 ) );
                        out.write( '\n' );
                        log.debug( "[{}] write headers {}", filename, logId.headers );
                    }
                } else {
                    log.info( "[{}] file exists v{}, new version {}", filename, version, version + 1 );
                    version += 1;
                    if( version > MAX_VERSION ) throw new IllegalStateException( "version > " + MAX_VERSION );
                    write( buffer, offset, length, error );
                    return;
                }
            log.trace( "writing {} bytes to {}", length, this );
            out.write( buffer, offset, length );

        } catch( IOException e ) {
            log.error( "Cannot write to " + filename, e );
            try {
                closeOutput();
            } finally {
                out = null;
            }
            throw new LoggerException( e );
        }
    }

    private Path filename() {
        return logDirectory.resolve( lastPattern );
    }

    public synchronized void refresh( boolean forceSync ) {
        refresh( false );
    }

    public synchronized void refresh( boolean forceSync ) {
        var currentPattern = currentPattern();
        if( forceSync || !Objects.equals( this.lastPattern, currentPattern ) ) {
            var patternWithPreviousVersion = currentPattern( version - 1 );
            if( !Objects.equals( patternWithPreviousVersion, this.lastPattern ) ) {
                version = 1;
            }
            currentPattern = currentPattern();

            log.trace( "force {} change pattern from '{}' to '{}'", forceSync, this.lastPattern, currentPattern );
            closeOutput();

            lastPattern = currentPattern;
        }
    }

    private String currentPattern() {
        return currentPattern( version );
    }

    private String currentPattern( int version ) {
        return logId.fileName( filePattern, new DateTime( DateTimeZone.UTC ), timestamp, version );
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + filename();
    }
}
