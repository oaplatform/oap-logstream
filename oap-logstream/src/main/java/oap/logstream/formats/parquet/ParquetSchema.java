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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.Builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class ParquetSchema extends AbstractSchema<org.apache.parquet.schema.Types.Builder<?, ?>> {
    private static final HashMap<Types, Function<List<Builder<?, ?>>, Builder<?, ?>>> types = new HashMap<>();

    static {
        types.put( Types.BOOLEAN, children -> org.apache.parquet.schema.Types.required( BOOLEAN ) );
        types.put( Types.BYTE, children -> org.apache.parquet.schema.Types.required( INT32 ).as( LogicalTypeAnnotation.intType( 8, true ) ) );
        types.put( Types.SHORT, children -> org.apache.parquet.schema.Types.required( INT32 ).as( LogicalTypeAnnotation.intType( 16, true ) ) );
        types.put( Types.INTEGER, children -> org.apache.parquet.schema.Types.required( INT32 ) );
        types.put( Types.LONG, children -> org.apache.parquet.schema.Types.required( INT64 ) );
        types.put( Types.FLOAT, children -> org.apache.parquet.schema.Types.required( FLOAT ) );
        types.put( Types.DOUBLE, children -> org.apache.parquet.schema.Types.required( DOUBLE ) );
        types.put( Types.STRING, children -> org.apache.parquet.schema.Types.required( BINARY ).as( LogicalTypeAnnotation.stringType() ) );
        types.put( Types.DATE, children -> org.apache.parquet.schema.Types.required( INT32 ).as( LogicalTypeAnnotation.dateType() ) );
        types.put( Types.DATETIME, children -> org.apache.parquet.schema.Types.required( INT64 ).as( LogicalTypeAnnotation.timestampType( true, MILLIS ) ) );
        types.put( Types.LIST, children -> org.apache.parquet.schema.Types.requiredList().element( ( Type ) children.get( 0 ).named( "element" ) ) );
        types.put( Types.ENUM, children -> org.apache.parquet.schema.Types.required( BINARY ).as( LogicalTypeAnnotation.stringType() ) );
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

    public void setString( ParquetSimpleGroup group, String index, String value ) {
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
            if( type.asPrimitiveType().getPrimitiveTypeName() == DOUBLE ) {
                group.add( index, Double.parseDouble( value ) );
            } else
                group.add( index, Float.parseFloat( value ) );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation ) {
            group.add( index, value );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation ) {
            long ms = Dates.FORMAT_DATE.parseMillis( value );
            group.add( index, ( int ) ( ms / 24L / 60 / 60 / 1000 ) );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ) {
            group.add( index, Dates.PARSER_MULTIPLE_DATETIME.parseMillis( value ) );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation ) {
            var listType = type.asGroupType().getType( 0 ).asGroupType().getType( 0 );
            Group listGroup = group.addGroup( index );
            for( var item : TsvArray.parse( value ) ) {
                setString( listGroup.addGroup( "list" ), 0, item, listType );
            }
        } else if( logicalTypeAnnotation == null && type.isPrimitive() && type.asPrimitiveType().getPrimitiveTypeName() == INT64 ) {
            group.add( index, Long.parseLong( value ) );
        } else
            throw new IllegalStateException( "Unknown type: " + type + ", logical: " + type.getLogicalTypeAnnotation() );
    }

    public static String toString( Type type, ParquetSimpleGroup group, int x, int y ) {
        LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();

        if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation ) {
            return Dates.FORMAT_DATE.print( group.getInteger( x, y ) * 24L * 60 * 60 * 1000 );
        } else if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation ) {
            var list = new ArrayList<>( group.getFieldRepetitionCount( x ) );
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
