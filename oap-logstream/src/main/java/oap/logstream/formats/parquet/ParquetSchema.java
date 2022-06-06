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
import oap.tsv.TsvArray;
import oap.util.Dates;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.Builder;

import java.util.ArrayList;
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

    public void setString( SimpleGroup group, String index, String value ) {
        Type type = group.getType().getType( index );
        int fieldIndex = group.getType().getFieldIndex( index );

        setString( group, fieldIndex, value, type );
    }

    private void setString( Group group, int index, String value, Type type ) {
        if( value == null ) return;

        LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
        if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation ) {
            int bitWidth = ( ( LogicalTypeAnnotation.IntLogicalTypeAnnotation ) logicalTypeAnnotation ).getBitWidth();
            switch( bitWidth ) {
                case 8 -> group.add( index, Byte.parseByte( value ) );
                case 16 -> group.add( index, Short.parseShort( value ) );
                case 32 -> group.add( index, Integer.parseInt( value ) );
                default -> group.add( index, Long.parseLong( value ) );
            }

        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation ) {
            if( type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.DOUBLE ) {
                group.add( index, Double.parseDouble( value ) );
            } else
                group.add( index, Float.parseFloat( value ) );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation ) {
            group.add( index, value );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation ) {
            long ms = Dates.FORMAT_DATE.parseMillis( value );
            group.add( index, ( int ) ( ms / 24L / 60 / 60 / 1000 ) );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ) {
            group.add( index, Long.parseLong( value ) );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation ) {
            var listType = type.asGroupType().getType( 0 );
            Group listGroup = group.addGroup( index );
            for( var item : TsvArray.parse( value ) ) {
                setString( listGroup, 0, item, listType );
            }
        } else if( logicalTypeAnnotation == null && type.isPrimitive() && type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64 ) {
            group.add( index, Long.parseLong( value ) );
        } else
            throw new IllegalStateException( "Unknown type: " + type + ", logical: " + type.getLogicalTypeAnnotation() );
    }

    public static String toString( Type type, SimpleGroup group, int x, int y ) {
        LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();

        if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation ) {
            return Dates.FORMAT_DATE.print( group.getInteger( x, y ) * 24L * 60 * 60 * 1000 );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation ) {
            var list = new ArrayList<Object>( group.getFieldRepetitionCount( x ) );
            for( var listIndex = 0; listIndex < group.getFieldRepetitionCount( x ); listIndex++ ) {
                var listItemType = ( ( GroupType ) type ).getType( 0 );
                LogicalTypeAnnotation listItemLogicalTypeAnnotation = listItemType.getLogicalTypeAnnotation();

                if( listItemLogicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                    || listItemLogicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation
                    || listItemLogicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ) {
                    list.add( "'" + toString( listItemType, group, x, listIndex ) + "'" );
                }
            }
            return TsvArray.print( list, Dates.FORMAT_DATE );
        }

        return group.getValueToString( x, y );
    }
}
