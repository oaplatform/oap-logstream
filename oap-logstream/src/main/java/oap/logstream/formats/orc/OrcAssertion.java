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

import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.logstream.formats.MemoryInputStreamWrapper;
import oap.util.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.util.StreamWrapperFileSystem;
import org.assertj.core.api.AbstractAssert;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class OrcAssertion extends AbstractAssert<OrcAssertion, OrcAssertion.OrcData> {
    protected OrcAssertion( OrcAssertion.OrcData data ) {
        super( data, OrcAssertion.class );
    }

    public static OrcAssertion assertOrc( Path path ) {
        try {
            return new OrcAssertion( new OrcData( Files.readAllBytes( path ) ) );
        } catch( IOException e ) {
            throw Throwables.propagate( e );
        }
    }

    public static Row row( Object... cols ) {
        return new Row( cols );
    }

    @SuppressWarnings( "checkstyle:NoWhitespaceAfter" ) // checkstyle bug
    private static Object toJavaObject( ColumnVector columnVector, TypeDescription typeDescription, int rowId ) {
        return switch( typeDescription.getCategory() ) {
            case BOOLEAN -> ( ( LongColumnVector ) columnVector ).vector[rowId] == 1;
            case BYTE -> ( byte ) ( ( LongColumnVector ) columnVector ).vector[rowId];
            case SHORT -> ( short ) ( ( LongColumnVector ) columnVector ).vector[rowId];
            case INT -> ( int ) ( ( LongColumnVector ) columnVector ).vector[rowId];
            case LONG -> ( ( LongColumnVector ) columnVector ).vector[rowId];
            case FLOAT -> ( float ) ( ( DoubleColumnVector ) columnVector ).vector[rowId];
            case DOUBLE -> ( ( DoubleColumnVector ) columnVector ).vector[rowId];
            case DATE -> new DateTime( ( ( DateColumnVector ) columnVector ).vector[rowId] * 24 * 60 * 60 * 1000, UTC );
            case TIMESTAMP -> new DateTime( ( ( TimestampColumnVector ) columnVector ).getTimestampAsLong( rowId ), UTC );
            case STRING -> ( ( BytesColumnVector ) columnVector ).toString( rowId );
            case LIST -> {
                ListColumnVector listColumnVector = ( ListColumnVector ) columnVector;

                var size = listColumnVector.lengths[rowId];
                var offset = listColumnVector.offsets[rowId];

                var list = new ArrayList<>();
                for( var i = 0; i < size; i++ ) {
                    list.add( toJavaObject( listColumnVector.child, typeDescription.getChildren().get( 0 ), ( int ) ( offset + i ) ) );
                }

                yield list;
            }
            default -> throw new IllegalStateException( "Unknown category type " + typeDescription );
        };
    }

    public OrcAssertion hasHeaders( String... headers ) {
        assertThat( actual.headers ).contains( headers );
        return this;
    }

    public OrcAssertion hasHeaders( Iterable<String> headers ) {
        assertThat( actual.headers ).containsAll( headers );
        return this;
    }

    public OrcAssertion containOnlyHeaders( String... headers ) {
        assertThat( actual.headers ).containsOnly( headers );
        return this;
    }

    public final OrcAssertion containsExactlyInAnyOrder( Row... rows ) {
        assertThat( actual.data ).containsExactlyInAnyOrder( rows );

        return this;
    }

    public final OrcAssertion contains( Row... rows ) {
        assertThat( actual.data ).contains( rows );

        return this;
    }

    public final OrcAssertion containsExactly( Row... rows ) {
        assertThat( actual.data ).containsExactly( rows );

        return this;
    }

    public final OrcAssertion containsOnly( Row... rows ) {
        assertThat( actual.data ).containsOnly( rows );

        return this;
    }

    public final OrcAssertion containsOnlyOnce( Row... rows ) {
        assertThat( actual.data ).containsOnlyOnce( rows );

        return this;
    }

    public final OrcAssertion containsAnyOf( Row... rows ) {
        assertThat( actual.data ).containsAnyOf( rows );

        return this;
    }

    @ToString
    @EqualsAndHashCode
    public static class Row {
        private final ArrayList<Object> cols = new ArrayList<>();

        public Row( Object... cols ) {
            this.cols.addAll( List.of( cols ) );
        }

        public Row( List<Object> cols ) {
            this.cols.addAll( cols );
        }
    }

    @ToString
    public static class OrcData {
        public final ArrayList<String> headers = new ArrayList<>();
        public final ArrayList<Row> data = new ArrayList<>();
        public TypeDescription schema;

        public OrcData( byte[] buffer ) throws IOException {
            Configuration conf = new Configuration();
            FSDataInputStream fsdis = new FSDataInputStream( new MemoryInputStreamWrapper( new ByteArrayInputStream( buffer ), buffer.length ) );
            var isFs = new StreamWrapperFileSystem( fsdis, new org.apache.hadoop.fs.Path( "my file" ), buffer.length, conf );

            try( Reader reader = OrcFile.createReader( new org.apache.hadoop.fs.Path( "my file" ), OrcFile.readerOptions( conf ).filesystem( isFs ).useUTCTimestamp( true ) );
                 RecordReader rows = reader.rows() ) {
                this.headers.addAll( reader.getSchema().getFieldNames() );

                schema = reader.getSchema();

                TypeDescription readSchema = reader.getSchema();
                VectorizedRowBatch rowBatch = readSchema.createRowBatch();

                while( rows.nextBatch( rowBatch ) ) {
                    ColumnVector[] cols = rowBatch.cols;
                    for( var rowId = 0; rowId < rowBatch.size; rowId++ ) {
                        var row = new Row();

                        for( var x = 0; x < this.headers.size(); x++ ) {
                            ColumnVector columnVector = cols[x];
                            row.cols.add( toJavaObject( columnVector, schema.getChildren().get( x ), rowId ) );
                        }

                        this.data.add( row );
                    }
                }
            }
        }

    }
}
