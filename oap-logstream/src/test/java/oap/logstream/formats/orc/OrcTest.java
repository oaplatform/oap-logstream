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

package oap.logstream.formats.orc;

import oap.dictionary.DictionaryParser;
import oap.dictionary.DictionaryRoot;
import oap.io.IoStreams;
import oap.logstream.formats.MemoryInputStreamWrapper;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import oap.tsv.Tsv;
import oap.tsv.TsvStream;
import oap.util.Dates;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.util.StreamWrapperFileSystem;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static oap.benchmark.Benchmark.benchmark;

@SuppressWarnings( "checkstyle:NoWhitespaceAfter" ) // checkstyle bug
public class OrcTest extends Fixtures {
    public static final int rows = 1000000;
    public static final int samples = 10;
    public static final int experiments = 5;

    public OrcTest() {
        fixture( TestDirectoryFixture.FIXTURE );
    }

    public static void main( String[] args ) throws IOException {
        String source = args[0];
        String datamodel = args[1];
        String type = args[2];
        String out = FilenameUtils.removeExtension( source ) + ".orc";

        DictionaryRoot dictionaryRoot = DictionaryParser.parse( Paths.get( datamodel ), DictionaryParser.INCREMENTAL_ID_STRATEGY );
        var schema = new Schema( dictionaryRoot.getValue( type ) );

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal( conf );

        if( Files.exists( Paths.get( out ) ) )
            Files.delete( Paths.get( out ) );

        try( Writer writer = OrcFile.createWriter( new Path( out ), OrcFile.writerOptions( conf ).fileSystem( fs )
            .setSchema( schema ).compress( CompressionKind.ZSTD ).useUTCTimestamp( true ) ) ) {

            VectorizedRowBatch batch = schema.createRowBatch( 1024 * 64 );

            TsvStream tsvStream = Tsv.tsv.fromPath( Paths.get( source ) ).withHeaders();
            var headers = tsvStream.headers();
            var index = new HashMap<String, Integer>();
            for( int i = 0; i < headers.size(); i++ ) index.put( headers.get( i ), i );

            try( var stream = tsvStream.stripHeaders().toStream() ) {
                stream.forEach( cols -> {
                    try {
                        var num = batch.size++;

                        for( int i = 0; i < schema.getFieldNames().size(); i++ ) {
                            var header = schema.getFieldNames().get( i );
                            var idx = index.get( header );
                            schema.setString( batch.cols[i], header, num, idx != null ? cols.get( idx ) : null );
                        }

                        if( batch.size == batch.getMaxSize() ) {
                            writer.addRowBatch( batch );
                            batch.reset();
                        }
                    } catch( Exception e ) {
                        e.printStackTrace();
                        throw new RuntimeException( e );
                    }
                } );
            }

            if( batch.size != 0 ) {
                writer.addRowBatch( batch );
            }
        } finally {
            var name = FilenameUtils.getName( out );
            var parent = FilenameUtils.getFullPathNoEndSeparator( out );
            java.nio.file.Path crcPath = Paths.get( parent + "/." + name + ".crc" );
            if( Files.exists( crcPath ) )
                Files.delete( crcPath );
        }
    }

    @Test( enabled = false )
    public void testPerformaceTsv() throws IOException {
        benchmark( "tsv", samples, i -> {
            var file = TestDirectoryFixture.testPath( "test" + i + ".tsv.zstd" );

            try( var os = IoStreams.out( file, IoStreams.Encoding.ZSTD ) ) {
                for( var row = 0; row < rows; row++ ) {
                    DateTime dateTime = new DateTime( row );
                    long fl = row;
                    long fl1 = row % 10;
                    long bl = 99999999L + row;
                    int fi = row;
                    String v1 = "1234567dsihgsiughsiupdr" + row + "huig hsrg";
                    String v2 = "1-2-3-4-5-6-7-d-s-i-h-g-s-i-u-g-h-s-iupdr" + row + "huig hsrg";

                    String line = Dates.FORMAT_SIMPLE_CLEAN.print( dateTime )
                        + "\t" + fl
                        + "\t" + fl1
                        + "\t" + bl
                        + "\t" + fi
                        + "\t" + v1
                        + "\t" + v2;
                    os.write( ( line + "\n" ).getBytes() );
                }
            }
        } ).experiments( experiments )
            .warming( 0 )
            .run();

        System.out.println( "size = " + FileUtils.byteCountToDisplaySize( Files.size( TestDirectoryFixture.testPath( "test1.tsv.zstd" ) ) ) );
    }

    @Test( enabled = false )
    public void testPerformaceOrc() throws IOException {
        DictionaryRoot dictionaryRoot = DictionaryParser.parse( "/datamodel.conf", DictionaryParser.INCREMENTAL_ID_STRATEGY );
        var schema = new Schema( dictionaryRoot.getValue( "PERF" ) );
        VectorizedRowBatch batch = schema.createRowBatch( 1024 * 64 );

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal( conf );

        AtomicInteger f = new AtomicInteger();

        benchmark( "orc", samples, i -> {
            var file = TestDirectoryFixture.testPath( "test" + f.incrementAndGet() + ".orc" );

            try( Writer writer = OrcFile.createWriter( new Path( file.toString() ), OrcFile.writerOptions( conf ).fileSystem( fs )
                .setSchema( schema ).compress( CompressionKind.ZSTD ).useUTCTimestamp( true ) );
            ) {

                for( int row = 0; row < rows; row++ ) {
                    DateTime dateTime = new DateTime( row );
                    long fl = row;
                    long fl1 = row % 10;
                    long bl = 99999999L + row;
                    int fi = row;
                    String v1 = "1234567dsihgsiughsiupdr" + row + "huig hsrg";
                    String v2 = "1-2-3-4-5-6-7-d-s-i-h-g-s-i-u-g-h-s-iupdr" + row + "huig hsrg";

                    var num = batch.size++;

                    schema.setString( batch.cols[0], "DATETIME", num, dateTime );
                    schema.setString( batch.cols[1], "L", num, fi );
                    schema.setString( batch.cols[2], "L1", num, fl1 );
                    schema.setString( batch.cols[3], "BL", num, bl );
                    schema.setString( batch.cols[4], "FI", num, fi );
                    schema.setString( batch.cols[5], "V1", num, v1 );
                    schema.setString( batch.cols[6], "V2", num, v2 );

                    if( batch.size == batch.getMaxSize() ) {
                        writer.addRowBatch( batch );
                        batch.reset();
                    }
                }

                if( batch.size != 0 ) {
                    writer.addRowBatch( batch );
                }
            }
        } ).experiments( experiments )
            .warming( 0 )
            .run();


        System.out.println( "size = " + FileUtils.byteCountToDisplaySize( Files.size( TestDirectoryFixture.testPath( "test1.orc" ) ) ) );
    }


    @Test
    public void testRW() throws IOException {
        DictionaryRoot dictionaryRoot = DictionaryParser.parse( "/datamodel.conf", DictionaryParser.INCREMENTAL_ID_STRATEGY );
        var schema = new Schema( dictionaryRoot.getValue( "TEST" ) );

        var time = DateTimeUtils.currentTimeMillis();
        System.out.println( "time = " + new Timestamp( time ) );

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal( conf );

        var file = TestDirectoryFixture.testPath( "test.orc" );

        try( Writer writer = OrcFile.createWriter( new Path( file.toString() ), OrcFile.writerOptions( conf ).fileSystem( fs )
            .setSchema( schema ).compress( CompressionKind.ZSTD ).useUTCTimestamp( true ) ) ) {
            VectorizedRowBatch batch = schema.createRowBatch( 1024 * 64 );

            for( int i = 0; i < 3; i++ ) {
                var num = batch.size++;

                ( ( TimestampColumnVector ) batch.cols[0] ).set( num, new Timestamp( time + i ) );
                ( ( BytesColumnVector ) batch.cols[1] ).setVal( num, "ID_SOURCE".getBytes() );
                ( ( BytesColumnVector ) batch.cols[2] ).setVal( num, "ID_STRING_WITH_LENGTH".getBytes() );
                ( ( LongColumnVector ) batch.cols[3] ).vector[num] = i;

                if( batch.size == batch.getMaxSize() ) {
                    writer.addRowBatch( batch );
                    batch.reset();
                }
            }
            if( batch.size != 0 ) {
                writer.addRowBatch( batch );
            }
        }

        var crc = TestDirectoryFixture.testPath( ".test.orc.crc" );
        Files.delete( crc );


        try( Reader reader = OrcFile.createReader( new Path( file.toString() ), OrcFile.readerOptions( conf ).filesystem( fs ).useUTCTimestamp( true ) );
             RecordReader rows = reader.rows() ) {
            read( reader, rows );
        }

        try( FileInputStream fis = new FileInputStream( file.toString() );
             MemoryInputStreamWrapper mw = MemoryInputStreamWrapper.wrap( fis );
             FSDataInputStream fsdis = new FSDataInputStream( mw ) ) {

            var isFs = new StreamWrapperFileSystem( fsdis, new Path( "my file" ), mw.length(), conf );

            try( Reader reader = OrcFile.createReader( new Path( "my file" ), OrcFile.readerOptions( conf ).filesystem( isFs ).useUTCTimestamp( true ) );
                 RecordReader rows = reader.rows() ) {
                read( reader, rows );
            }
        }
    }

    private void read( Reader reader, RecordReader rows ) throws IOException {
        TypeDescription readSchema = reader.getSchema();
        List<String> fieldNames = readSchema.getFieldNames();
        System.out.println( fieldNames );
        VectorizedRowBatch rowBatch = readSchema.createRowBatch();

        int row = 0;

        while( rows.nextBatch( rowBatch ) ) {
            ColumnVector[] cols = rowBatch.cols;
            for( var y = 0; y < rowBatch.size; y++ ) {
                System.out.println( "row = " + row + ":" );

                for( var x = 0; x < fieldNames.size(); x++ ) {
                    System.out.print( "    " + fieldNames.get( x ) + " = " );
                    switch( fieldNames.get( x ) ) {
                        case "ID_DATETIME" -> System.out.println( ( ( TimestampColumnVector ) cols[x] ).asScratchTimestamp( y ) );
                        case "ID_SOURCE" -> System.out.println( ( ( BytesColumnVector ) cols[x] ).toString( y ) );
                        case "ID_STRING_WITH_LENGTH" -> System.out.println( ( ( BytesColumnVector ) cols[x] ).toString( y ) );
                        case "ID_LONG" -> System.out.println( ( ( LongColumnVector ) cols[x] ).vector[y] );
                        default -> System.out.println( "UNKNOWN  " + fieldNames.get( x ) );
                    }
                }
                row++;
            }
        }
    }
}
