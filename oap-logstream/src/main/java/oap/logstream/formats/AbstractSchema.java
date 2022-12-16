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

package oap.logstream.formats;

import com.google.common.base.Preconditions;
import lombok.ToString;
import oap.dictionary.Dictionary;
import oap.template.Types;
import oap.util.Dates;
import oap.util.Lists;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractSchema<TSchema> {
    public final TSchema schema;
    protected final HashMap<String, FieldInfo<TSchema>> defaultValuesMap = new HashMap<>();
    protected final ArrayList<FieldInfo<TSchema>> defaultValuesList;

    @SuppressWarnings( "unchecked" )
    protected AbstractSchema( TSchema schema, Dictionary dictionary, HashMap<Types, Function<List<TSchema>, TSchema>> types ) {
        this.schema = schema;

        defaultValuesList = new ArrayList<>( dictionary.getValues().size() );
        var i = 0;

        var fields = new LinkedHashMap<String, TSchema>();

        for( var col : dictionary.getValues() ) {
            Object typeObj = col.getProperty( "type" ).orElse( null );
            Preconditions.checkArgument( typeObj instanceof String || typeObj instanceof List,
                "[" + col.getId() + "] type must be string or list<string>" );
            List<String> type = typeObj instanceof List<?> ? ( List<String> ) typeObj : List.of( typeObj.toString() );
            Preconditions.checkArgument( type.size() > 0 );

            TSchema fieldType = null;
            for( var typeIdx = type.size() - 1; typeIdx >= 0; typeIdx-- ) {
                var typeEnum = Types.valueOf( type.get( typeIdx ) );
                var func = types.get( typeEnum );
                fieldType = func.apply( fieldType != null ? List.of( fieldType ) : List.of() );
            }

            fields.put( col.getId(), fieldType );

            Object defaultValue = col.getProperty( "default" ).orElseThrow( () -> new IllegalArgumentException( col.getId() + ": default is required" ) );

            FieldInfo<TSchema> fieldInfo = new FieldInfo<>( defaultValue, fieldType, Lists.map( type, Types::valueOf ) );
            defaultValuesMap.put( col.getId(), fieldInfo );
            defaultValuesList.add( fieldInfo );
        }

        setFields( fields );
    }

    protected abstract void setFields( LinkedHashMap<String, TSchema> fields );

    protected static Timestamp toTimestamp( Object value ) {
        if( value instanceof Timestamp valueTimestamp ) return valueTimestamp;
        else if( value instanceof DateTime valueDateTime )
            return new Timestamp( valueDateTime.getMillis() );
        else if( value instanceof Long longValue )
            return new Timestamp( longValue );
        else
            return new Timestamp( Dates.FORMAT_SIMPLE.parseMillis( value.toString() ) );
    }

    protected String enumToString( Object value ) {
        if( value instanceof Enum<?> valueEnum ) return valueEnum.name();

        return toString( value );
    }

    protected String toString( Object value ) {
        return value.toString();
    }

    protected double toDouble( Object value ) {
        return value instanceof Number ? ( ( Number ) value ).doubleValue() : Double.parseDouble( value.toString() );
    }

    @SuppressWarnings( "unchecked" )
    protected List<String> toList( Object value, List<Types> types ) {
        if( value instanceof List<?> ) return ( List<String> ) value;

        var arrayStr = value.toString().trim();
        var array = arrayStr.substring( 1, arrayStr.length() - 1 );

        var data = StringUtils.splitPreserveAllTokens( array, ',' );

        return List.of( data );
    }

    protected long toDate( Object value ) {
        if( value instanceof DateTime )
            return ( ( DateTime ) value ).getMillis() / 24 / 60 / 60 / 1000;
        else if( value instanceof Long )
            return ( long ) value;
        else
            return Dates.FORMAT_DATE.parseMillis( value.toString() ) / 24 / 60 / 60 / 1000;
    }

    protected short toShort( Object value ) {
        return value instanceof Number ? ( ( Number ) value ).shortValue() : Short.parseShort( value.toString() );
    }

    protected long toBoolean( Object value ) {
        if( value instanceof Boolean booleanValue ) return booleanValue ? 1 : 0;
        else return Boolean.parseBoolean( value.toString() ) ? 1 : 0;
    }

    protected int toByte( Object value ) {
        return value instanceof Number ? ( ( Number ) value ).byteValue() : Byte.parseByte( value.toString() );
    }

    protected int toInt( Object value ) {
        return value instanceof Number ? ( ( Number ) value ).intValue() : Integer.parseInt( value.toString() );
    }

    protected long toLong( Object value ) {
        return value instanceof Number ? ( ( Number ) value ).longValue() : Long.parseLong( value.toString() );
    }

    protected float toFloat( Object value ) {
        return value instanceof Number ? ( ( Number ) value ).floatValue() : Float.parseFloat( value.toString() );
    }

    @ToString
    protected static class FieldInfo<TSchema> {
        public final Object defaultValue;
        public final List<Types> type;
        public final TSchema schema;

        public FieldInfo( Object defaultValue, TSchema schema, List<Types> type ) {
            this.defaultValue = defaultValue;
            this.schema = schema;
            this.type = type;
        }
    }
}
