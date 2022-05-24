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
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.joda.time.DateTimeUtils;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

public class ParquetTest {
    @Test
    public void testRW() throws IOException {
        DictionaryRoot dictionaryRoot = DictionaryParser.parse( "/datamodel.conf", DictionaryParser.INCREMENTAL_ID_STRATEGY );
        var schema = new ParquetSchema( dictionaryRoot.getValue( "TEST" ) );

        var time = DateTimeUtils.currentTimeMillis();
        System.out.println( "time = " + new Timestamp( time ) );


        Configuration conf = new Configuration();

        var file = TestDirectoryFixture.testPath( "test.parquet" );
        try( ParquetWriter<GenericData.Record> writer = AvroParquetWriter
            .<GenericData.Record>builder( HadoopOutputFile.fromPath( new Path( file.toString() ), conf ) )
            .withSchema( schema.schema )
            .build() ) {

            for( int i = 0; i < 3; i++ ) {
                var record = new GenericData.Record( schema.schema );

                record.put( 0, time + i );
                record.put( 1, "ID_SOURCE" );
                record.put( 2, "ID_STRING_WITH_LENGTH" );
                record.put( 3, i );

                writer.write( record );
            }
        }

        try( ParquetReader<GenericData.Record> reader = AvroParquetReader
            .<GenericData.Record>builder( HadoopInputFile.fromPath( new Path( file.toString() ), conf ) )
            .build() ) {
            GenericData.Record record;
            int row = 1;
            while( ( record = reader.read() ) != null ) {
                read( record, row );
                row++;
            }
        }

        try( FileInputStream fis = new FileInputStream( file.toString() ) ) {

            try( ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder( new ParquetInputFile( fis ) ).build() ) {
                GenericData.Record record;

                int row = 1;
                while( ( record = reader.read() ) != null ) {
                    read( record, row );
                    row++;
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
