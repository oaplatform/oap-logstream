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

package oap.testng;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.logstream.MemoryLoggerBackend;
import oap.logstream.data.object.BinaryObjectLogger;
import oap.template.BinaryUtils;
import oap.util.IndexTranslatingList;
import oap.util.Lists;
import org.apache.commons.collections4.IteratorUtils;
import org.assertj.core.api.AbstractAssert;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BinaryLogAssertion extends AbstractAssert<BinaryLogAssertion, MemoryLoggerBackend> {

    private final List<List<Object>> rows;
    private final BinaryObjectLogger.TypedBinaryLogger<?> logger;

    private BinaryLogAssertion( MemoryLoggerBackend memoryLoggerBackend, String logType, BinaryObjectLogger.TypedBinaryLogger<?> logger ) throws IOException {
        super( memoryLoggerBackend, BinaryLogAssertion.class );

        rows = BinaryUtils.read( memoryLoggerBackend.loggedBytes( logId -> logId.logType.equals( logType ) ) );
        this.logger = logger;
    }

    public static BinaryLogAssertion assertBinaryLog( MemoryLoggerBackend memoryLoggerBackend, String logType, BinaryObjectLogger.TypedBinaryLogger<?> logger ) throws IOException {
        return new BinaryLogAssertion( memoryLoggerBackend, logType, logger );
    }

    public static Row row( Object... cols ) {
        return new Row( cols );
    }

    public static Header header( String... cols ) {
        return new Header( cols );
    }

    public final BinaryLogAssertion hasHeader( String headerName ) {
        assertThat( logger.headers ).contains( headerName );

        return this;
    }

    public final BinaryLogAssertion hasHeaders( String... headerNames ) {
        assertThat( logger.headers ).contains( headerNames );

        return this;
    }

    public final BinaryLogAssertion hasHeaders( Header headers ) {
        return hasHeaders( headers.cols );
    }

    public final BinaryLogAssertion hasHeaders( Iterable<String> headerNames ) {
        assertThat( logger.headers ).contains( IteratorUtils.toArray( headerNames.iterator(), String.class ) );

        return this;
    }

    public final BinaryLogAssertion containsOnlyHeaders( String... headerNames ) {
        assertThat( logger.headers ).containsOnly( headerNames );

        return this;
    }

    public final BinaryLogAssertion containsOnlyHeaders( Iterable<String> headerNames ) {
        assertThat( logger.headers ).containsOnly( IteratorUtils.toArray( headerNames.iterator(), String.class ) );

        return this;
    }

    public BinaryLogAssertion containsExactlyInAnyOrderEntriesOf( Header header, Row... rows ) {
        hasHeaders( header );
        for( var row : rows ) {
            assertThat( row.cols )
                .withFailMessage( "entries length doesnt match headers" )
                .hasSize( header.size() );
        }
        assertThat( filter( this.rows, header.cols ) )
            .containsExactlyInAnyOrderElementsOf( Lists.map( rows, r -> r.cols ) );

        return this;
    }

    public BinaryLogAssertion containsAnyEntriesOf( Header header, Row... rows ) {
        hasHeaders( header.cols );
        for( var row : rows ) {
            assertThat( row.cols )
                .withFailMessage( "entries length doesnt match headers" )
                .hasSize( header.size() );
        }

        assertThat( filter( this.rows, header.cols ) )
            .containsAnyElementsOf( Lists.map( rows, r -> r.cols ) );
        return this;
    }

    public BinaryLogAssertion containsOnlyOnceEntriesOf( Header header, Row... rows ) {
        hasHeaders( header );
        for( var row : rows ) {
            assertThat( row.cols )
                .withFailMessage( "entries length doesnt match headers" )
                .hasSize( header.size() );
        }
        assertThat( filter( this.rows, header.cols ) )
            .containsOnlyOnceElementsOf( Lists.map( rows, r -> r.cols ) );

        return this;
    }

    public BinaryLogAssertion doesNotContainAnyEntriesOf( Header header, Row... rows ) {
        hasHeaders( header );
        for( var row : rows ) {
            assertThat( row.cols )
                .withFailMessage( "entries length doesnt match headers" )
                .hasSize( header.size() );
        }

        assertThat( filter( this.rows, header.cols ) )
            .doesNotContainAnyElementsOf( Lists.map( rows, r -> r.cols ) );
        return this;
    }

    private List<List<Object>> filter( List<List<Object>> rows, List<String> headers ) {
        var indexes = Lists.indices( List.of( logger.headers ), IteratorUtils.toArray( headers.iterator(), String.class ) );

        return Lists.map( rows, row -> new IndexTranslatingList<>( row, indexes ) );
    }

    @ToString
    @EqualsAndHashCode
    public static class Row {
        private final List<Object> cols;

        public Row( Object... cols ) {
            this.cols = List.of( cols );
        }
    }

    @ToString
    @EqualsAndHashCode
    public static class Header {
        public final List<String> cols;

        public Header( String... cols ) {
            this.cols = List.of( cols );
        }

        public int size() {
            return cols.size();
        }
    }
}
