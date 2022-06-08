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
import oap.logstream.formats.parquet.ParquetSchema;
import oap.logstream.formats.parquet.ParquetWriteBuilder;
import oap.tsv.Tsv;
import oap.tsv.TsvStream;
import oap.util.Throwables;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings( "UnstableApiUsage" )
@Slf4j
public class ParquetWriter extends AbstractWriter<org.apache.parquet.hadoop.ParquetWriter<Group>> {
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

    private final ParquetSchema schema;
    private final MessageType messageType;

    public ParquetWriter( Path logDirectory, Dictionary model, String filePattern, LogId logId, int bufferSize, Timestamp timestamp, int maxVersions ) {
        super( logDirectory, model, filePattern, logId, bufferSize, timestamp, true, maxVersions );

        schema = new ParquetSchema( model.getValue( logId.logType ) );
        messageType = ( MessageType ) schema.schema.named( "logger" );
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

                    GroupWriteSupport.setSchema( messageType, conf );

                    out = new ParquetWriteBuilder( HadoopOutputFile.fromPath( new org.apache.hadoop.fs.Path( filename.toString() ), conf ) )
                        .withConf( conf )
                        .build();

                    new LogMetadata( logId ).writeFor( filename );
                } else {
                    log.info( "[{}] file exists v{}", filename, version );
                    version += 1;
                    if( version > maxVersions ) throw new IllegalStateException( "version > " + maxVersions );
                    write( buffer, offset, length, error );
                    return;
                }
            log.trace( "writing {} bytes to {}", length, this );
            writeFromTsv( out, buffer, offset, length, StringUtils.splitPreserveAllTokens( logId.headers, '\t' ) );
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

    private void writeFromTsv( org.apache.parquet.hadoop.ParquetWriter<Group> out, byte[] buffer, int offset, int length, String[] headers ) throws IOException {
        TsvStream tsvStream = Tsv.tsv.from( buffer, offset, length );
        Iterator<List<String>> iterator = tsvStream.toStream().iterator();

        while( iterator.hasNext() ) {
            var line = iterator.next();
            SimpleGroup group = new SimpleGroup( messageType );

            for( var colId = 0; colId < line.size(); colId++ ) {
                var header = headers[colId];

                if( messageType.containsField( header ) )
                    schema.setString( group, header, line.get( colId ) );
            }
            out.write( group );
        }
    }

    @Override
    protected void closeOutput() throws LoggerException {
        Path orcFile = outFilename;

        try {
            super.closeOutput();
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
