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

import oap.dictionary.DictionaryRoot;
import oap.logstream.AbstractLoggerBackend;
import oap.logstream.Logger;
import oap.reflect.TypeRef;
import oap.template.TemplateAccumulator;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Map;

public abstract class AbstractObjectLogger<D, TOut, TAccumulator, TTemplateAccumulator extends TemplateAccumulator<TOut, TAccumulator, TTemplateAccumulator>> extends Logger {
    private final ObjectLogRenderer<D, TOut, TAccumulator, TTemplateAccumulator> renderer;
    private final String name;

    public AbstractObjectLogger( AbstractLoggerBackend backend, DictionaryRoot model, TTemplateAccumulator templateAccumulator,
                                 Path tmpPath, String id, String tag, String name, TypeRef<D> typeRef ) {
        this( backend, new ObjectLogModel<>( model, tmpPath ), templateAccumulator, id, tag, name, typeRef );
    }

    private AbstractObjectLogger( AbstractLoggerBackend backend,
                                  ObjectLogModel<D, TOut, TAccumulator, TTemplateAccumulator> logModel,
                                  TTemplateAccumulator templateAccumulator,
                                  String id, String tag, String name, TypeRef<D> typeRef ) {
        super( backend );
        this.name = name;
        this.renderer = logModel.renderer( typeRef, templateAccumulator, id, tag );
    }

    public void log( @Nonnull D data ) {
        this.log( prefix( data ), substitutions( data ), name, 0,
            renderer.headers(), renderer.types(), renderer.render( data ) );
    }

    @Nonnull
    public abstract String prefix( @Nonnull D data );

    @Nonnull
    public abstract Map<String, String> substitutions( @Nonnull D data );

}
