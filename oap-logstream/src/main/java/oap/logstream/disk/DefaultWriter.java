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

import com.google.common.io.CountingOutputStream;
import lombok.extern.slf4j.Slf4j;
import oap.io.IoStreams;
import oap.io.IoStreams.Encoding;
import oap.logstream.LogId;
import oap.logstream.LoggerException;
import oap.logstream.Timestamp;
import oap.template.BinaryInputStream;
import oap.template.TemplateAccumulatorString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class DefaultWriter extends AbstractWriter<CountingOutputStream> {
    private final String dateTime32Format;

    public DefaultWriter( Path logDirectory, String filePattern, LogId logId,
                          String dateTime32Format,
                          int bufferSize, Timestamp timestamp,
                          boolean withHeaders, int maxVersions ) {
        super( logDirectory, filePattern, logId, bufferSize, timestamp, withHeaders, maxVersions );

        this.dateTime32Format = dateTime32Format;
    }

    public DefaultWriter( Path logDirectory, String filePattern, LogId logId,
                          String dateTime32Format, int bufferSize, Timestamp timestamp, int maxVersions ) {
        super( logDirectory, filePattern, logId, bufferSize, timestamp, maxVersions );

        this.dateTime32Format = dateTime32Format;
    }

    public synchronized void write( byte[] buffer, Consumer<String> error ) throws LoggerException {
        write( buffer, 0, buffer.length, error );
    }

    @Override
    public synchronized void write( byte[] buffer, int offset, int length, Consumer<String> error ) throws LoggerException {
        if( closed ) {
            throw new LoggerException( "writer is already closed!" );
        }
        try {
            refresh();
            var filename = filename();
            if( out == null )
                if( !java.nio.file.Files.exists( filename ) ) {
                    log.info( "[{}] open new file v{}", filename, version );
                    outFilename = filename;
                    out = new CountingOutputStream( IoStreams.out( filename, Encoding.from( filename ), bufferSize ) );
                    new LogMetadata( logId ).writeFor( filename );
                    if( withHeaders ) {
                        out.write( String.join( "\t", logId.headers ).getBytes( UTF_8 ) );
                        out.write( '\n' );
                        log.debug( "[{}] write headers {}", filename, logId.headers );
                    }
                } else {
                    log.info( "[{}] file exists v{}", filename, version );
                    version += 1;
                    if( version > maxVersions ) throw new IllegalStateException( "version > " + maxVersions );
                    write( buffer, offset, length, error );
                    return;
                }
            log.trace( "writing {} bytes to {}", length, this );

            convertToTsv( buffer, offset, length, line -> out.write( line ) );

        } catch( IOException e ) {
            log.error( e.getMessage(), e );
            try {
                closeOutput();
            } finally {
                outFilename = null;
                out = null;
            }
            throw new LoggerException( e );
        }
    }

    private void convertToTsv( byte[] buffer, int offset, int length, IOExceptionConsumer<byte[]> cons ) throws IOException {
        var bis = new BinaryInputStream( new ByteArrayInputStream( buffer, offset, length ) );

        var sb = new StringBuilder();
        TemplateAccumulatorString templateAccumulatorString = new TemplateAccumulatorString( sb, dateTime32Format );
        Object obj = bis.readObject();
        while( obj != null ) {
            while( obj != null && obj != BinaryInputStream.EOL ) {
                if( sb.length() > 0 ) sb.append( '\t' );
                templateAccumulatorString.accept( obj );
                obj = bis.readObject();
            }
            cons.accept( templateAccumulatorString.addEol( obj == BinaryInputStream.EOL ).getBytes() );
            sb.delete( 0, sb.length() );
            obj = bis.readObject();
        }
    }

    @FunctionalInterface
    public interface IOExceptionConsumer<T> {
        void accept( T t ) throws IOException;
    }
}
