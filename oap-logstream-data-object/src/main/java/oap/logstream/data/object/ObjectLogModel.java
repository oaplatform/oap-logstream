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

package oap.logstream.data.object;

import lombok.extern.slf4j.Slf4j;
import oap.io.Resources;
import oap.logstream.data.AbstractLogModel;
import oap.reflect.TypeRef;
import oap.template.TemplateEngine;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;
import static oap.template.ErrorStrategy.ERROR;

@Slf4j
public class ObjectLogModel<D> extends AbstractLogModel<D> {
    private final TemplateEngine engine;

    public ObjectLogModel( Path location, Path tmpPath ) {
        super( location );
        this.engine = new TemplateEngine( tmpPath );
    }

    public ObjectLogModel( String resourceLocation, Path tmpPath ) {
        super( Resources.url( ObjectLogModel.class, resourceLocation )
            .orElseThrow( () -> new IllegalArgumentException( resourceLocation ) ) );
        this.engine = new TemplateEngine( tmpPath );
    }

    public ObjectLogRenderer<D> renderer( TypeRef<D> typeRef, String id, String tag ) {
        var value = requireNonNull( model.getValue( id ), "configuration for " + id + " is not found" );

        var headers = new StringJoiner( "\t" );
        var expressions = new ArrayList<String>();

        for( var field : value.getValues( d -> d.getTags().contains( tag ) && d.containsProperty( "path" ) ) ) {
            var name = field.getId();
            var path = ( String ) field.getProperty( "path" ).orElseThrow();
            var defaultValue = field.getProperty( "default" )
                .map( v -> v instanceof String ? "\"" + ( ( String ) v ).replace( "\"", "\\\"" ) + '"' : v )
                .orElseThrow( () -> new IllegalStateException( "default not found for " + id + "/" + name ) );

            expressions.add( "${" + path + " ?? " + defaultValue + "}" );
            headers.add( name );
        }

        var template = String.join( "\t", expressions );
        var renderer = engine.getTemplate(
            "Log" + StringUtils.capitalize( id ),
            typeRef,
            template,
            new OptimizedAccumulatorString(),
            ERROR,
            null );
        return new ObjectLogRenderer<>( renderer, headers.toString() );
    }
}
