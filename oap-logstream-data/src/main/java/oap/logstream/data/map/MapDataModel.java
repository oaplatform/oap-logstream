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

package oap.logstream.data.map;

import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import oap.dictionary.Dictionary;
import oap.dictionary.DictionaryParser;
import oap.dictionary.DictionaryRoot;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;
import static oap.dictionary.DictionaryParser.INCREMENTAL_ID_STRATEGY;

@Slf4j
public class MapDataModel {
    private final DictionaryRoot model;

    @SneakyThrows
    public MapDataModel( @Nonnull Path location ) {
        Preconditions.checkArgument( location.toFile().exists(), "datamodel configuration does not exists " + location );

        log.debug( "loading {}", location );
        this.model = DictionaryParser.parse( location.toUri().toURL(), INCREMENTAL_ID_STRATEGY );
    }

    public LogRenderer renderer( String id, String tag ) {
        Dictionary dictionary = requireNonNull( this.model.getValue( id ) );
        StringJoiner headers = new StringJoiner( "\t" );
        List<String> expressions = new ArrayList<>();
        for( Dictionary field : dictionary.getValues( d -> d.getTags().contains( tag ) ) ) {
            headers.add( field.getId() );
            expressions.add( field.<String>getProperty( "path" )
                .orElseThrow( () -> new IllegalArgumentException( "undefined property path for " + field.getId() ) ) );
        }
        return new LogRenderer( headers.toString(), expressions );
    }

}
