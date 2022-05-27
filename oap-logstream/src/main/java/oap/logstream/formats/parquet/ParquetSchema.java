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

import oap.dictionary.Dictionary;
import oap.logstream.Types;
import oap.logstream.formats.AbstractSchema;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.Builder;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;

import static oap.logstream.Types.BYTE;
import static oap.logstream.Types.DATE;
import static oap.logstream.Types.DATETIME;
import static oap.logstream.Types.DOUBLE;
import static oap.logstream.Types.ENUM;
import static oap.logstream.Types.FLOAT;
import static oap.logstream.Types.INTEGER;
import static oap.logstream.Types.LIST;
import static oap.logstream.Types.LONG;
import static oap.logstream.Types.SHORT;
import static oap.logstream.Types.STRING;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;

public class ParquetSchema extends AbstractSchema<org.apache.parquet.schema.Types.Builder<?, ?>> {
    private static final HashMap<Types, Function<List<Builder<?, ?>>, Builder<?, ?>>> types = new HashMap<>();

    static {
        types.put( Types.BOOLEAN, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED ) );
        types.put( BYTE, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED ).as( LogicalTypeAnnotation.intType( 8, true ) ) );
        types.put( SHORT, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED ).as( LogicalTypeAnnotation.intType( 16, true ) ) );
        types.put( INTEGER, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED ) );
        types.put( LONG, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED ) );
        types.put( FLOAT, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.REQUIRED ) );
        types.put( DOUBLE, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED ) );
        types.put( STRING, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED ).as( LogicalTypeAnnotation.stringType() ) );
        types.put( DATE, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED ).as( LogicalTypeAnnotation.dateType() ) );
        types.put( DATETIME, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED ).as( LogicalTypeAnnotation.timestampType( true, MILLIS ) ) );
        types.put( LIST, children -> org.apache.parquet.schema.Types.list( Type.Repetition.REQUIRED ).element( ( Type ) children.get( 0 ).named( "list" ) ) );
        types.put( ENUM, children -> org.apache.parquet.schema.Types.primitive( PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED ).as( LogicalTypeAnnotation.stringType() ) );
    }

    public ParquetSchema( Dictionary dictionary ) {
        super( org.apache.parquet.schema.Types.buildMessage(), dictionary, types );
    }

    @Override
    protected void setFields( LinkedHashMap<String, Builder<?, ?>> fields ) {
        fields.forEach( ( n, b ) -> {
            ( ( org.apache.parquet.schema.Types.MessageTypeBuilder ) schema ).addField( ( Type ) b.named( n ) );
        } );
    }
}
