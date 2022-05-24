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

import com.google.common.base.Preconditions;
import oap.dictionary.Dictionary;
import oap.logstream.Types;
import oap.logstream.formats.AbstractSchema;
import oap.util.Dates;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static oap.logstream.Types.BOOLEAN;
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

@SuppressWarnings( "checkstyle:NoWhitespaceAfter" )
public class OrcSchema extends AbstractSchema<TypeDescription> {
    private static final HashMap<Types, Function<List<TypeDescription>, TypeDescription>> types = new HashMap<>();

    static {
        types.put( BOOLEAN, children -> TypeDescription.createBoolean() );
        types.put( BYTE, children -> TypeDescription.createByte() );
        types.put( SHORT, children -> TypeDescription.createShort() );
        types.put( INTEGER, children -> TypeDescription.createInt() );
        types.put( LONG, children -> TypeDescription.createLong() );
        types.put( FLOAT, children -> TypeDescription.createFloat() );
        types.put( DOUBLE, children -> TypeDescription.createDouble() );
        types.put( STRING, children -> TypeDescription.createString() );
        types.put( DATE, children -> TypeDescription.createDate() );
        types.put( DATETIME, children -> TypeDescription.createTimestamp() );
        types.put( LIST, children -> TypeDescription.createList( children.get( 0 ) ) );
        types.put( ENUM, children -> TypeDescription.createString() );
    }

    public OrcSchema( Dictionary dictionary ) {
        super( new TypeDescription( Category.STRUCT ), dictionary, types );
    }

    public static String toString( ColumnVector columnVector, TypeDescription typeDescription, int row ) {
        return switch( typeDescription.getCategory() ) {
            case BOOLEAN, BYTE, SHORT, INT, LONG -> String.valueOf( ( ( LongColumnVector ) columnVector ).vector[row] );
            case STRING, BINARY -> ( ( BytesColumnVector ) columnVector ).toString( row );
            case FLOAT, DOUBLE -> String.valueOf( ( ( DoubleColumnVector ) columnVector ).vector[row] );
            case DATE ->
                Dates.FORMAT_DATE.print( Dates.d( ( int ) ( ( LongColumnVector ) columnVector ).vector[row] ) );
            case TIMESTAMP ->
                Dates.FORMAT_SIMPLE_CLEAN.print( ( ( TimestampColumnVector ) columnVector ).asScratchTimestamp( row ).getTime() );
            case LIST -> {
                ListColumnVector listColumnVector = ( ListColumnVector ) columnVector;
                StringJoiner sj = new StringJoiner( ",", "[", "]" );

                var childTypeDescription = typeDescription.getChildren().get( 0 );

                for( var i = 0; i < listColumnVector.lengths[row]; i++ ) {
                    int offset = ( int ) listColumnVector.offsets[row];
                    sj.add( toString( listColumnVector.child, childTypeDescription, offset + i ) );
                }

                yield sj.toString();
            }
            default -> throw new IllegalArgumentException( "Unknown category " + typeDescription.getCategory() );
        };
    }

    @SuppressWarnings( "checkstyle:ModifiedControlVariable" )
    public static boolean[] getInclude( List<String> cols, List<TypeDescription> typeDescriptions, List<String> includeCols ) {

        int size = 1;
        for( var td : typeDescriptions ) {
            if( td.getCategory() == Category.LIST ) {
                size += 1;
            } else if( td.getCategory() == Category.MAP ) {
                size += 2;
            }
            size += 1;
        }

        boolean[] include = new boolean[size];
        if( includeCols.isEmpty() ) Arrays.fill( include, true );
        else {
            include[0] = true;
            for( int idx = 0, inc = 1; idx < cols.size(); idx++, inc++ ) {
                TypeDescription typeDescription = typeDescriptions.get( idx );

                if( includeCols.contains( cols.get( idx ) ) ) {
                    include[inc] = true;
                    if( typeDescription.getCategory() == Category.LIST ) {
                        include[inc + 1] = true;
                        inc++;
                    } else if( typeDescription.getCategory() == Category.MAP ) {
                        include[inc + 1] = true;
                        include[inc + 2] = true;
                        inc += 2;
                    }
                }
            }
        }

        return include;
    }

    public void set( ColumnVector col, int num, short value ) {
        ( ( LongColumnVector ) col ).vector[num] = value;
    }

    public void set( ColumnVector col, int num, int value ) {
        ( ( LongColumnVector ) col ).vector[num] = value;
    }

    public void set( ColumnVector col, int num, long value ) {
        ( ( LongColumnVector ) col ).vector[num] = value;
    }

    public void set( ColumnVector col, int num, boolean value ) {
        ( ( LongColumnVector ) col ).vector[num] = value ? 1 : 0;
    }

    public void set( ColumnVector col, int num, float value ) {
        ( ( DoubleColumnVector ) col ).vector[num] = value;
    }

    public void set( ColumnVector col, int num, double value ) {
        ( ( DoubleColumnVector ) col ).vector[num] = value;
    }

    public void set( ColumnVector col, int num, DateTime value ) {
        ( ( TimestampColumnVector ) col ).set( num, new Timestamp( value.getMillis() ) );
    }

    public void set( ColumnVector col, int num, Timestamp value ) {
        ( ( TimestampColumnVector ) col ).set( num, value );
    }

    public void setTimestamp( ColumnVector col, int num, long value ) {
        ( ( TimestampColumnVector ) col ).set( num, new Timestamp( value ) );
    }

    public void setDate( ColumnVector col, int num, DateTime dateTime ) {
        ( ( DateColumnVector ) col ).vector[num] = dateTime.getMillis() / 24 / 60 / 60 / 1000;
    }

    @SuppressWarnings( "checkstyle:OverloadMethodsDeclarationOrder" )
    public void set( ColumnVector col, int num, String value ) {
        ( ( BytesColumnVector ) col ).setVal( num, value.getBytes( UTF_8 ) );
    }

    public void set( ColumnVector col, int num, List<String> value ) {
        ListColumnVector listCol = ( ListColumnVector ) col;

        if( listCol.childCount + value.size() > listCol.child.isNull.length ) {
            listCol.child.ensureSize( listCol.childCount * 2, true );
        }

        listCol.lengths[num] = value.size();
        listCol.offsets[num] = listCol.childCount;

        for( var i = 0; i < value.size(); i++ ) {
            set( listCol.child, i + listCol.childCount, value.get( i ) );
        }

        listCol.childCount += value.size();
    }

    public void setString( ColumnVector col, String header, int num, Object value ) {
        var idx = schema.getFieldNames().indexOf( header );
        Preconditions.checkArgument( idx >= 0, "'" + header + "' not found" );

        FieldInfo fieldInfo = defaultValuesMap.get( header );

        setString( col, num, value, fieldInfo );
    }

    public void setString( ColumnVector col, int rowId, int colId, Object value ) {
        FieldInfo<TypeDescription> fieldInfo = defaultValuesList.get( colId );

        setString( col, rowId, value, fieldInfo );
    }

    @SuppressWarnings( "checkstyle:ParameterAssignment" )
    private void setString( ColumnVector col, int rowId, Object value, FieldInfo<TypeDescription> fieldInfo ) {
        if( value == null ) value = fieldInfo.defaultValue;

        switch( fieldInfo.type.get( 0 ) ) {
            case BOOLEAN -> set( col, rowId, toBoolean( value ) );
            case BYTE -> set( col, rowId, toByte( value ) );
            case SHORT -> set( col, rowId, toShort( value ) );
            case INTEGER -> set( col, rowId, toInt( value ) );
            case LONG -> set( col, rowId, toLong( value ) );
            case FLOAT -> set( col, rowId, toFloat( value ) );
            case DOUBLE -> set( col, rowId, toDouble( value ) );
            case STRING -> set( col, rowId, toString( value ) );
            case DATE -> set( col, rowId, toDate( value ) );
            case DATETIME -> set( col, rowId, toTimestamp( value ) );
            case LIST -> set( col, rowId, toList( value, fieldInfo.type.subList( 1, fieldInfo.type.size() ) ) );
            case ENUM -> set( col, rowId, enumToString( value ) );
            default -> throw new IllegalStateException( "unknown type " + fieldInfo.type );
        }
    }

    private void setList( ListColumnVector listCol, int rowId, List<String> listString ) {
        if( listCol.childCount + listString.size() > listCol.child.isNull.length ) {
            listCol.child.ensureSize( listCol.childCount * 2, true );
        }

        listCol.lengths[rowId] = listString.size();
        listCol.offsets[rowId] = listCol.childCount;

        for( var i = 0; i < listString.size(); i++ ) {
            set( listCol.child, i + listCol.childCount, listString.get( i ) );
        }

        listCol.childCount += listString.size();
    }

    @Override
    protected void setFields( LinkedHashMap<String, TypeDescription> fields ) {
        fields.forEach( schema::addField );
    }
}
