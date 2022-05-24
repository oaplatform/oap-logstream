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

import lombok.extern.slf4j.Slf4j;
import oap.dictionary.Dictionary;
import oap.logstream.LogId;
import oap.logstream.LoggerException;
import oap.logstream.Timestamp;
import oap.logstream.formats.orc.OrcSchema;
import oap.tsv.Tsv;
import oap.tsv.TsvStream;
import oap.util.Throwables;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings( "UnstableApiUsage" )
@Slf4j
public class OrcWriter extends AbstractWriter<Writer> {
    private static final FileSystem fs;
    private static final Configuration conf;

    static {
        try {
            conf = new Configuration();
            fs = FileSystem.getLocal( conf );
        } catch( IOException e ) {
            log.error( e.getMessage(), e );
            throw Throwables.propagate( e );
        }
    }

    private final OrcSchema schema;
    private final VectorizedRowBatch batch;

    public OrcWriter( Path logDirectory, Dictionary model, String filePattern, LogId logId, int bufferSize, Timestamp timestamp, int maxVersions ) {
        super( logDirectory, model, filePattern, logId, bufferSize, timestamp, true, maxVersions );

        schema = new OrcSchema( model.getValue( logId.logType ) );
        batch = schema.schema.createRowBatch( 1024 * 64 );
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

                    out = OrcFile.createWriter( new org.apache.hadoop.fs.Path( filename.toString() ), OrcFile.writerOptions( conf ).fileSystem( fs )
                        .setSchema( schema.schema ).compress( CompressionKind.ZSTD ).useUTCTimestamp( true ) );

                    new LogMetadata( logId ).writeFor( filename );
                } else {
                    log.info( "[{}] file exists v{}", filename, version );
                    version += 1;
                    if( version > maxVersions ) throw new IllegalStateException( "version > " + maxVersions );
                    write( buffer, offset, length, error );
                    return;
                }
            log.trace( "writing {} bytes to {}", length, this );
            writeFromTsv( out, buffer, offset, length );
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

    private void writeFromTsv( Writer out, byte[] buffer, int offset, int length ) throws IOException {
        TsvStream tsvStream = Tsv.tsv.from( buffer, offset, length );
        Iterator<List<String>> iterator = tsvStream.toStream().iterator();

        while( iterator.hasNext() ) {
            var rowId = batch.size++;

            var line = iterator.next();

            for( var colId = 0; colId < line.size(); colId++ ) {
                ColumnVector colVector = batch.cols[colId];

                schema.setString( colVector, rowId, colId, line.get( colId ) );
            }

            if( batch.size == batch.getMaxSize() ) {
                out.addRowBatch( batch );
                batch.reset();
            }
        }
    }

    @Override
    protected void closeOutput() throws LoggerException {
        Path orcFile = outFilename;

        try {
            if( batch.size != 0 ) {
                out.addRowBatch( batch );
            }

            super.closeOutput();
        } catch( IOException e ) {
            throw new LoggerException( e );
        } finally {
            if( orcFile != null ) {
                var name = FilenameUtils.getName( orcFile.toString() );
                var parent = FilenameUtils.getFullPathNoEndSeparator( orcFile.toString() );
                java.nio.file.Path crcPath = Paths.get( parent + "/." + name + ".crc" );

                if( Files.exists( crcPath ) )
                    try {
                        Files.delete( crcPath );
                    } catch( IOException e ) {
                        log.error( e.getMessage(), e );
                    }
            }
        }
    }
}
