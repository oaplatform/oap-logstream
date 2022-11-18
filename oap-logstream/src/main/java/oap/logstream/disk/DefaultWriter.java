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
import oap.dictionary.Dictionary;
import oap.io.IoStreams;
import oap.io.IoStreams.Encoding;
import oap.logstream.LogId;
import oap.logstream.LoggerException;
import oap.logstream.Timestamp;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class DefaultWriter extends AbstractWriter<CountingOutputStream> {
    public DefaultWriter( Path logDirectory, Dictionary model, String filePattern, LogId logId, int bufferSize, Timestamp timestamp,
                          boolean withHeaders, int maxVersions ) {
        super( logDirectory, model, filePattern, logId, bufferSize, timestamp, withHeaders, maxVersions );
    }

    public DefaultWriter( Path logDirectory, Dictionary model, String filePattern, LogId logId, int bufferSize, Timestamp timestamp, int maxVersions ) {
        super( logDirectory, model, filePattern, logId, bufferSize, timestamp, maxVersions );
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
                        out.write( logId.headers.getBytes( UTF_8 ) );
                        out.write( '\n' );
                        log.debug( "[{}] write headers {}", filename, logId.headers );
                    }
                } else {
                    log.trace( "[{}] file exists", filename );

                    var metadata = LogMetadata.readFor( filename );

                    log.info( "[{}] file exists v{}", filename, version );
                    version += 1;
                    if( version > maxVersions ) throw new IllegalStateException( "version > " + maxVersions );
                    write( buffer, offset, length, error );
                    return;
                }
            log.trace( "writing {} bytes to {}", length, this );
            out.write( buffer, offset, length );

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
}
