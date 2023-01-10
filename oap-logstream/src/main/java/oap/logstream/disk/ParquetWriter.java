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
import oap.logstream.LogId;
import oap.logstream.LoggerException;
import oap.logstream.Timestamp;
import oap.logstream.formats.parquet.ParquetSimpleGroup;
import oap.logstream.formats.parquet.ParquetWriteBuilder;
import oap.template.BinaryInputStream;
import oap.util.Lists;
import oap.util.Throwables;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

@SuppressWarnings( "UnstableApiUsage" )
@Slf4j
public class ParquetWriter extends AbstractWriter<org.apache.parquet.hadoop.ParquetWriter<Group>> {
    private static final FileSystem fs;
    private static final Configuration conf;

    private static final HashMap<Byte, Function<List<Types.Builder<?, ?>>, Types.Builder<?, ?>>> types = new HashMap<>();

    static {
        types.put( oap.template.Types.BOOLEAN.id, children -> org.apache.parquet.schema.Types.required( BOOLEAN ) );
        types.put( oap.template.Types.BYTE.id, children -> org.apache.parquet.schema.Types.required( INT32 ).as( LogicalTypeAnnotation.intType( 8, true ) ) );
        types.put( oap.template.Types.SHORT.id, children -> org.apache.parquet.schema.Types.required( INT32 ).as( LogicalTypeAnnotation.intType( 16, true ) ) );
        types.put( oap.template.Types.INTEGER.id, children -> org.apache.parquet.schema.Types.required( INT32 ).as( LogicalTypeAnnotation.intType( 32, true ) ) );
        types.put( oap.template.Types.LONG.id, children -> org.apache.parquet.schema.Types.required( INT64 ).as( LogicalTypeAnnotation.intType( 64, true ) ) );
        types.put( oap.template.Types.FLOAT.id, children -> org.apache.parquet.schema.Types.required( FLOAT ) );
        types.put( oap.template.Types.DOUBLE.id, children -> org.apache.parquet.schema.Types.required( DOUBLE ) );
        types.put( oap.template.Types.RAW.id, children -> org.apache.parquet.schema.Types.required( BINARY ).as( LogicalTypeAnnotation.stringType() ) );
        types.put( oap.template.Types.STRING.id, children -> org.apache.parquet.schema.Types.required( BINARY ).as( LogicalTypeAnnotation.stringType() ) );
        types.put( oap.template.Types.DATE.id, children -> org.apache.parquet.schema.Types.required( INT32 ).as( LogicalTypeAnnotation.dateType() ) );
        types.put( oap.template.Types.DATETIME.id, children -> org.apache.parquet.schema.Types.required( INT64 ) );
//        types.put( Types.DADATETIME64.id, children -> org.apache.parquet.schema.Types.required( INT64 ).as( LogicalTypeAnnotation.timestampType( true, MILLIS ) ) );
        types.put( oap.template.Types.LIST.id, children -> org.apache.parquet.schema.Types.requiredList().element( ( Type ) children.get( 0 ).named( "element" ) ) );
//        types.put( Types.ENUM.id, children -> org.apache.parquet.schema.Types.required( BINARY ).as( LogicalTypeAnnotation.stringType() ) );
    }

    static {
        try {
            conf = new Configuration();
            fs = FileSystem.getLocal( conf );
        } catch( IOException e ) {
            log.error( e.getMessage(), e );
            throw Throwables.propagate( e );
        }
    }

    //    private ParquetSchema schema;
    private final MessageType messageType;

    public ParquetWriter( Path logDirectory, String filePattern, LogId logId, int bufferSize, Timestamp timestamp, int maxVersions )
        throws IllegalArgumentException {
        super( logDirectory, filePattern, logId, bufferSize, timestamp, true, maxVersions );


        Types.MessageTypeBuilder messageTypeBuilder = Types.buildMessage();


        for( var i = 0; i < logId.headers.length; i++ ) {
            var header = logId.headers[i];
            var type = logId.types[i];


            Types.Builder<?, ?> fieldType = null;
            for( var idx = type.length - 1; idx >= 0; idx-- ) {
                Function<List<Types.Builder<?, ?>>, Types.Builder<?, ?>> builderFunction = types.get( type[idx] );
                Preconditions.checkArgument( builderFunction != null, "" );
                fieldType = builderFunction.apply( fieldType != null ? List.of( fieldType ) : List.of() );
            }

            messageTypeBuilder.addField( ( Type ) fieldType.named( header ) );
        }

//        schema = new ParquetSchema( logTypeDictionary );
        messageType = messageTypeBuilder.named( "logger" );
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

                    new LogMetadata( logId ).withProperty( "VERSION", logId.getHashWithVersion( version ) ).writeFor( filename );
                } else {
                    log.info( "[{}] file exists v{}", filename, version );
                    version += 1;
                    if( version > maxVersions ) throw new IllegalStateException( "version > " + maxVersions );
                    write( buffer, offset, length, error );
                    return;
                }
            log.trace( "writing {} bytes to {}", length, this );
            convertToParquet( buffer, offset, length, logId.types, logId.headers );
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

    private void convertToParquet( byte[] buffer, int offset, int length, byte[][] types, String[] headers ) throws IOException {
        var bis = new BinaryInputStream( new ByteArrayInputStream( buffer, offset, length ) );
        int col = 0;
        ParquetSimpleGroup group = new ParquetSimpleGroup( messageType );
        Object obj = bis.readObject();
        while( obj != null ) {
            while( obj != null && obj != BinaryInputStream.EOL ) {
                var colType = types[col];
                try {
                    addValue( col, obj, colType, 0, group );
                } catch( Exception e ) {
                    var type = types[col];
                    log.error( "header {} class {} type {}", headers[col], obj.getClass().getName(),
                        Lists.map( List.of( ArrayUtils.toObject( types[col] ) ), oap.template.Types::valueOf ) );
                    throw e;
                }
                obj = bis.readObject();
                col++;
            }
            out.write( group );
            col = 0;
            group = new ParquetSimpleGroup( messageType );
            obj = bis.readObject();
        }
    }

    private static void addValue( int col, Object obj, byte[] colType, int typeIdx, Group group ) {
        var type = colType[typeIdx];
        if( type == oap.template.Types.BOOLEAN.id ) {
            group.add( col, ( boolean ) obj );
        } else if( type == oap.template.Types.BYTE.id ) {
            group.add( col, ( byte ) obj );
        } else if( type == oap.template.Types.SHORT.id ) {
            group.add( col, ( short ) obj );
        } else if( type == oap.template.Types.INTEGER.id ) {
            group.add( col, ( int ) obj );
        } else if( type == oap.template.Types.LONG.id ) {
            group.add( col, ( long ) obj );
        } else if( type == oap.template.Types.FLOAT.id ) {
            group.add( col, ( float ) obj );
        } else if( type == oap.template.Types.DOUBLE.id ) {
            group.add( col, ( double ) obj );
        } else if( type == oap.template.Types.STRING.id ) {
            group.add( col, ( String ) obj );
        } else if( type == oap.template.Types.DATETIME.id ) {
            group.add( col, ( ( DateTime ) obj ).getMillis() / 1000 );
        } else if( type == oap.template.Types.LIST.id ) {
            var listGroup = group.addGroup( col );
            for( var item : ( List<?> ) obj ) {
                addValue( 0, item, colType, typeIdx + 1, listGroup.addGroup( "list" ) );
            }
        } else {
            throw new IllegalStateException( "Unknown type:" + type );
        }
    }


//    private void writeFromTsv( org.apache.parquet.hadoop.ParquetWriter<Group> out, byte[] buffer, int offset, int length, String[] headers ) throws IOException {
//        TsvStream tsvStream = Tsv.tsv.from( buffer, offset, length );
//        Iterator<List<String>> iterator = tsvStream.toStream().iterator();
//
//        while( iterator.hasNext() ) {
//            var line = iterator.next();
//            ParquetSimpleGroup group = new ParquetSimpleGroup( messageType );
//
//            for( var colId = 0; colId < line.size(); colId++ ) {
//                var header = headers[colId];
//
//                if( messageType.containsField( header ) )
//                    schema.setString( group, header, line.get( colId ) );
//            }
//            out.write( group );
//        }
//    }

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
