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
import oap.io.Files;
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
public class Writer implements Closeable {
    private final Path logDirectory;
    private final String filePattern;
    private final LogId logId;
    private final Timestamp timestamp;
    private final int bufferSize;
    private CountingOutputStream out;
    private String lastPattern;
    private final Stopwatch stopwatch = new Stopwatch();
    private int version = 1;

    public Writer( Path logDirectory, String filePattern, LogId logId, int bufferSize, Timestamp timestamp ) {
        this.logDirectory = logDirectory;
        this.filePattern = filePattern;

        Preconditions.checkArgument( filePattern.contains( "${" + "LOG_VERSION" + "}" ) );

        this.logId = logId;
        this.bufferSize = bufferSize;
        this.timestamp = timestamp;
        this.lastPattern = currentPattern();
        log.debug( "spawning {}", this );
    }

    @Override
    public void close() {
        log.debug( "closing {}", this );
        closeOutput();
    }

    private void closeOutput() throws LoggerException {
        if( out != null ) try {
            log.trace( "closing output {} ({} bytes)", this, out.getCount() );
            stopwatch.measure( out::flush );
            stopwatch.measure( out::close );
            Metrics.summary( "logstream_logging_server_bucket_size" ).record( out.getCount() );
            Metrics.summary( "logstream_logging_server_bucket_time_seconds" ).record( Dates.nanosToSeconds( stopwatch.elapsed() ) );
            out = null;
        } catch( IOException e ) {
            throw new LoggerException( e );
        }
    }

    public synchronized void write( byte[] buffer, Consumer<String> error ) throws LoggerException {
        write( buffer, 0, buffer.length, error );
    }

    public synchronized void write( byte[] buffer, int offset, int length, Consumer<String> error ) throws LoggerException {
        try {
            refresh();
            var filename = filename();
            if( out == null ) {
                var exists = java.nio.file.Files.exists( filename );

                if( !exists ) {
                    log.info( "[{}] open new file v{}", filename, version );
                    out = new CountingOutputStream( IoStreams.out( filename, Encoding.from( filename ), bufferSize ) );
                    new LogMetadata( logId ).writeFor( filename );
                    out.write( logId.headers.getBytes( UTF_8 ) );
                    out.write( '\n' );
                    log.debug( "[{}] write headers {}", filename, logId.headers );
                } else {
                    log.trace( "[{}] file exists", filename );

                    var metadata = LogMetadata.readFor( filename );

                    if( metadata.equals( new LogMetadata( logId ) ) ) {
                        if( Files.isFileEncodingValid( filename ) ) {
                            log.info( "[{}] open existing file v{}", filename, version );
                            out = new CountingOutputStream( IoStreams.out( filename, Encoding.from( filename ), bufferSize, true ) );
                        } else {
                            error.accept( "corrupted file, cannot append " + filename );
                            log.error( "corrupted file, cannot append {}", filename );
                            var newFile = logDirectory.resolve( ".corrupted" )
                                .resolve( logDirectory.relativize( filename ) );
                            Files.rename( filename, newFile );
                            LogMetadata.rename( filename, newFile );
                            this.out = new CountingOutputStream( IoStreams.out( filename, Encoding.from( filename ), bufferSize ) );
                            new LogMetadata( logId ).writeFor( filename );
                        }
                    } else {
                        log.info( "[{}] file exists v{}", filename, version );
                        version += 1;
                        if( version > 10 ) throw new IllegalStateException( "version > 10" );
                        write( buffer, offset, length, error );
                        return;
                    }
                }
            }
            log.trace( "writing {} bytes to {}", length, this );
            out.write( buffer, offset, length );

        } catch( IOException e ) {
            log.error( e.getMessage(), e );
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

    public synchronized void refresh() {
        var currentPattern = currentPattern();
        if( !Objects.equals( this.lastPattern, currentPattern ) ) {
            currentPattern = currentPattern();

            log.trace( "change pattern from '{}' to '{}'", this.lastPattern, currentPattern );
            closeOutput();
            lastPattern = currentPattern;
            version = 1;
        }
    }

    private String currentPattern() {
        return logId.fileName( filePattern, new DateTime( DateTimeZone.UTC ), timestamp, version );
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + filename();
    }
}
