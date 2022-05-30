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

package oap.logstream.formats.parquet;

import oap.dictionary.DictionaryParser;
import oap.dictionary.DictionaryRoot;
import oap.testng.TestDirectoryFixture;
import oap.util.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import static org.joda.time.DateTimeZone.UTC;

public class ParquetTest {
    @Test
    public void testRW() throws IOException {
        DictionaryRoot dictionaryRoot = DictionaryParser.parse( "/datamodel.conf", DictionaryParser.INCREMENTAL_ID_STRATEGY );
        var schema = new ParquetSchema( dictionaryRoot.getValue( "TEST" ) );

        var time = 1653579985423L;
        System.out.println( "time = " + new Timestamp( time ) );


        Configuration conf = new Configuration();
        MessageType messageType = ( MessageType ) schema.schema.named( "group" );
        GroupWriteSupport.setSchema( messageType, conf );

        var file = TestDirectoryFixture.testPath( "test.parquet" );

        try( ParquetWriter<Group> writer = new ParquetWriteBuilder( HadoopOutputFile.fromPath( new Path( file.toString() ), conf ) )
            .withConf( conf )
            .build() ) {

            for( long i = 0; i < 3; i++ ) {
                SimpleGroup simpleGroup = new SimpleGroup( messageType );

                simpleGroup.add( 0, time + i );
                simpleGroup.add( 1, "ID_SOURCE" );
                simpleGroup.add( 2, "ID_STRING_WITH_LENGTH" );
                simpleGroup.add( 3, i );

                writer.write( simpleGroup );
            }
        }

        try( ParquetFileReader reader = ParquetFileReader.open( HadoopInputFile.fromPath( new Path( file.toString() ), conf ) ) ) {
            read( reader );
        }

        try( FileInputStream fis = new FileInputStream( file.toString() );
             ParquetFileReader reader = ParquetFileReader.open( new ParquetInputFile( fis ) ) ) {

            read( reader );
        }

        ParquetAssertion.assertParquet( file )
            .hasHeaders( "ID_DATETIME", "ID_SOURCE", "ID_STRING_WITH_LENGTH", "ID_LONG" )
            .containsExactly(
                ParquetAssertion.row( new DateTime( 1653579985423L, UTC ), "ID_SOURCE", "ID_STRING_WITH_LENGTH", 0L ),
                ParquetAssertion.row( new DateTime( 1653579985424L, UTC ), "ID_SOURCE", "ID_STRING_WITH_LENGTH", 1L ),
                ParquetAssertion.row( new DateTime( 1653579985425L, UTC ), "ID_SOURCE", "ID_STRING_WITH_LENGTH", 2L )
            );

        ParquetAssertion.assertParquet( file, "ID_SOURCE", "ID_DATETIME", "ID_STRING_WITH_LENGTH" )
            .containsExactly(
                ParquetAssertion.row( "ID_SOURCE", new DateTime( 1653579985423L, UTC ), "ID_STRING_WITH_LENGTH" ),
                ParquetAssertion.row( "ID_SOURCE", new DateTime( 1653579985424L, UTC ), "ID_STRING_WITH_LENGTH" ),
                ParquetAssertion.row( "ID_SOURCE", new DateTime( 1653579985425L, UTC ), "ID_STRING_WITH_LENGTH" )
            );

    }

    private void read( ParquetFileReader reader ) throws IOException {
        MessageType messageType = reader.getFooter().getFileMetaData().getSchema();

        List<String> fieldNames = Lists.map( messageType.getFields(), Type::getName );
        System.out.println( fieldNames );

        PageReadStore pages;
        while( ( pages = reader.readNextRowGroup() ) != null ) {
            long rows = pages.getRowCount();

            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO( messageType );
            RecordReader<Group> recordReader = columnIO.getRecordReader( pages, new GroupRecordConverter( messageType ) );

            for( int i = 0; i < rows; i++ ) {
                SimpleGroup simpleGroup = ( SimpleGroup ) recordReader.read();

                for( var x = 0; x < fieldNames.size(); x++ ) {
                    System.out.print( "    " + fieldNames.get( x ) + " = " );
                    System.out.println( simpleGroup.getValueToString( x, 0 ) );
                }
            }
        }
    }

    private void read( GenericData.Record record, int row ) {
        Schema readSchema = record.getSchema();
        List<String> fieldNames = Lists.map( readSchema.getFields(), Schema.Field::name );
        System.out.println( fieldNames );


        System.out.println( "row = " + row + ":" );

        for( var x = 0; x < fieldNames.size(); x++ ) {
            System.out.print( "    " + fieldNames.get( x ) + " = " );
            System.out.println( record.get( x ) );
        }
    }
}
