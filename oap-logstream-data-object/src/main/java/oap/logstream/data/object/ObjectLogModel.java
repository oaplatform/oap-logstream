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

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import oap.dictionary.Dictionary;
import oap.dictionary.DictionaryRoot;
import oap.logstream.data.AbstractLogModel;
import oap.reflect.TypeRef;
import oap.template.TemplateAccumulator;
import oap.template.TemplateEngine;
import oap.template.TemplateException;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;
import static oap.template.ErrorStrategy.ERROR;

/**
 * oap-module:
 * ....
 * model = classpath(...) | path(...(.yaml.|conf|.json)) | file(...(.yaml|.conf|.json)) | url (...) | hocon({name = config, values = [...]}) | json(...) | yaml (...)
 *
 * @param <D>
 */
@Slf4j
public class ObjectLogModel<D, TOut, TAccumulator, TA extends TemplateAccumulator<TOut, TAccumulator, TA>> extends AbstractLogModel<D, TOut, TAccumulator, TA> {
    public static final String COLLECTION_SUFFIX = "_ARRAY";

    public static final HashMap<String, String> types = new HashMap<>();

    public boolean typeValidation = true;

    static {
        types.put( "DATETIME", "org.joda.time.DateTime" );
        types.put( "BOOLEAN", "java.lang.Boolean" );
        types.put( "ENUM", "java.lang.Enum" );
        types.put( "STRING", "java.lang.String" );
        types.put( "LONG", "java.lang.Long" );
        types.put( "INTEGER", "java.lang.Integer" );
        types.put( "SHORT", "java.lang.Short" );
        types.put( "FLOAT", "java.lang.Float" );
        types.put( "DOUBLE", "java.lang.Double" );
    }

    private final TemplateEngine engine;

    public ObjectLogModel( @Nonnull DictionaryRoot model, @Nonnull Path tmpPath ) {
        super( model );
        this.engine = new TemplateEngine( tmpPath );
    }

    public ObjectLogRenderer<D, TOut, TAccumulator, TA> renderer( TypeRef<D> typeRef, TA accumulator, String id, String tag ) {
        var value = requireNonNull( model.getValue( id ), "configuration for " + id + " is not found" );

        var headers = new StringJoiner( "\t" );
        var expressions = new ArrayList<String>();

        for( var field : value.getValues( d -> d.getTags().contains( tag ) && d.containsProperty( "path" ) ) ) {
            var name = field.getId();
            var path = checkStringAndGet( field, "path" );
            var fieldType = checkStringAndGet( field, "type" );
            var format = field.getProperty( "format" ).orElse( null );

            boolean collection = false;
            var idType = fieldType;
            if( idType.endsWith( COLLECTION_SUFFIX ) ) {
                collection = true;
                idType = idType.substring( 0, idType.length() - COLLECTION_SUFFIX.length() );
            }

            var javaType = types.get( idType );
            Preconditions.checkNotNull( javaType, "unknown type " + idType );

            Preconditions.checkNotNull( javaType, "unknown type " + idType );

            var defaultValue = field.getProperty( "default" )
                .orElseThrow( () -> new IllegalStateException( "default not found for " + id + "/" + name ) );

            var templateFunction = format != null ? "; format(\"" + format + "\")" : "";
            var comment = "model '" + id + "' id '" + name + "' type '" + fieldType + "' defaultValue '" + defaultValue + "'";
            var pDefaultValue =
                defaultValue instanceof String ? "\"" + ( ( String ) defaultValue ).replace( "\"", "\\\"" ) + '"'
                    : defaultValue;

            expressions.add( "${/* " + comment + " */" + toJavaType( javaType, collection ) + path + " ?? " + pDefaultValue + templateFunction + "}" );
            headers.add( name );
        }

        var template = String.join( "\t", expressions );
        var renderer = engine.getTemplate(
            "Log" + StringUtils.capitalize( id ),
            typeRef,
            template,
            accumulator.newInstance(),
            ERROR,
            null );
        return new ObjectLogRenderer<>( renderer, headers.toString() );
    }

    private static String checkStringAndGet( Dictionary dictionary, String fieldName ) {
        Object fieldObject = dictionary.getProperty( fieldName ).orElseThrow( () -> new TemplateException( dictionary.getId() + ": type is required" ) );
        Preconditions.checkArgument( fieldObject instanceof String, dictionary.getId() + ": type must be String, but is " + fieldObject.getClass() );
        return ( String ) fieldObject;
    }

    private String toJavaType( String javaType, boolean collection ) {
        if( !typeValidation ) return "";

        StringBuilder sb = new StringBuilder( "<" );
        if( collection ) sb.append( "java.util.Collection<" );
        sb.append( javaType );
        if( collection ) sb.append( ">" );
        sb.append( ">" );
        return sb.toString();
    }
}
