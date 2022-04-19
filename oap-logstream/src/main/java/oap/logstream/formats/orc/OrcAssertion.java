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
import oap.util.FastByteArrayOutputStream;
import oap.util.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class OrcAssertion extends AbstractAssert<OrcAssertion, OrcAssertion.OrcData> {
    protected OrcAssertion( OrcAssertion.OrcData data ) {
        super( data, OrcAssertion.class );
    }

    public static OrcAssertion assertOrc( Path path, String... headers ) {
        try {
            byte[] buffer = Files.readAllBytes( path );
            return new OrcAssertion( new OrcData( buffer, 0, buffer.length, List.of( headers ) ) );
        } catch( IOException e ) {
            throw Throwables.propagate( e );
        }
    }

    public static OrcAssertion assertOrc( InputStream inputStream, String... headers ) {
        try {
            var out = new FastByteArrayOutputStream();
            IOUtils.copy( inputStream, out );
            return new OrcAssertion( new OrcData( out.array, 0, out.length, List.of( headers ) ) );
        } catch( IOException e ) {
            throw Throwables.propagate( e );
        }
    }

    public static OrcAssertion assertOrc( String data, String... headers ) {
        try {
            byte[] bytes = data.getBytes( UTF_8 );
            return new OrcAssertion( new OrcData( bytes, 0, bytes.length, List.of( headers ) ) );
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
            case TIMESTAMP -> {
                TimestampColumnVector timestampColumnVector = ( TimestampColumnVector ) columnVector;
                var timestamp = new Timestamp( 0L );
                timestamp.setTime( timestampColumnVector.time[rowId] );
                timestamp.setNanos( timestampColumnVector.nanos[rowId] );
                yield timestamp;
            }
            case STRING, BINARY -> ( ( BytesColumnVector ) columnVector ).toString( rowId );
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

        @SuppressWarnings( "checkstyle:ModifiedControlVariable" )
        public OrcData( byte[] buffer, int offset, int length, List<String> includeCols ) throws IOException {
            Configuration conf = new Configuration();
            FSDataInputStream fsdis = new FSDataInputStream( MemoryInputStreamWrapper.wrap( new ByteArrayInputStream( buffer, offset, length ), buffer.length ) );
            var isFs = new StreamWrapperFileSystem( fsdis, new org.apache.hadoop.fs.Path( "my file" ), buffer.length, conf );

            try( Reader reader = OrcFile.createReader( new org.apache.hadoop.fs.Path( "my file" ), OrcFile.readerOptions( conf ).filesystem( isFs ).useUTCTimestamp( true ) ) ) {
                this.headers.addAll( reader.getSchema().getFieldNames() );
                TypeDescription readSchema = reader.getSchema();

                boolean[] include = Schema.getInclude( this.headers, readSchema.getChildren(), includeCols );
                RecordReader rows = reader.rows( new Reader.Options().include( include ) );

                schema = reader.getSchema();

                VectorizedRowBatch rowBatch = readSchema.createRowBatch();

                while( rows.nextBatch( rowBatch ) ) {
                    ColumnVector[] cols = rowBatch.cols;
                    for( var rowId = 0; rowId < rowBatch.size; rowId++ ) {
                        var row = new Row();

                        for( int x = 0, i = 1; x < this.headers.size(); x++, i++ ) {
                            ColumnVector columnVector = cols[x];
                            if( include[i] )
                                row.cols.add( toJavaObject( columnVector, schema.getChildren().get( x ), rowId ) );
                            if( columnVector instanceof ListColumnVector ) i++;
                            else if( columnVector instanceof MapColumnVector ) i += 2;
                        }

                        this.data.add( row );
                    }
                }

                if( !includeCols.isEmpty() ) {
                    headers.clear();
                    headers.addAll( includeCols );
                }
            }
        }

    }
}
