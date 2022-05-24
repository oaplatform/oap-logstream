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
import oap.util.Maps;
import org.apache.avro.Schema;

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

public class ParquetSchema extends AbstractSchema<Schema> {
    private static final HashMap<Types, Function<List<org.apache.avro.Schema>, org.apache.avro.Schema>> types = new HashMap<>();

    static {
        types.put( Types.BOOLEAN, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.BOOLEAN ) );
        types.put( BYTE, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.INT ) );
        types.put( SHORT, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.INT ) );
        types.put( INTEGER, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.INT ) );
        types.put( LONG, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.LONG ) );
        types.put( FLOAT, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.FLOAT ) );
        types.put( DOUBLE, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.DOUBLE ) );
        types.put( STRING, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.STRING ) );
        types.put( DATE, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.INT ) );
        types.put( DATETIME, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.LONG ) );
        types.put( LIST, children -> org.apache.avro.Schema.createArray( children.get( 0 ) ) );
        types.put( ENUM, children -> org.apache.avro.Schema.create( org.apache.avro.Schema.Type.STRING ) );
    }

    public ParquetSchema( Dictionary dictionary ) {
        super( org.apache.avro.Schema.createRecord( dictionary.getId(), "", dictionary.getId(), true ), dictionary, types
        );
    }

    @Override
    protected void setFields( LinkedHashMap<String, org.apache.avro.Schema> fields ) {
        this.schema.setFields( Maps.toList( fields, org.apache.avro.Schema.Field::new ) );
    }
}
