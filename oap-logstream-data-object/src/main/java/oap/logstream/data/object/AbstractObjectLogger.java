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

import oap.logstream.Logger;
import oap.logstream.AbstractLoggerBackend;
import oap.reflect.TypeRef;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Map;

public abstract class AbstractObjectLogger<D> extends Logger {
    private final ObjectLogRenderer<D> renderer;
    private final String name;

    public AbstractObjectLogger( AbstractLoggerBackend backend, Path modelLocation, Path tmpPath, String id, String tag, String name, TypeRef<D> typeRef ) {
        this( backend, DEFAULT_TIMESTAMP, modelLocation, tmpPath, id, tag, name, typeRef );
    }

    public AbstractObjectLogger( AbstractLoggerBackend backend, String resourceLocation, Path tmpPath, String id, String tag, String name, TypeRef<D> typeRef ) {
        this( backend, DEFAULT_TIMESTAMP, resourceLocation, tmpPath, id, tag, name, typeRef );
    }

    public AbstractObjectLogger( AbstractLoggerBackend backend, String timestampFormat, Path modelLocation, Path tmpPath, String id, String tag, String name, TypeRef<D> typeRef ) {
        this( backend, timestampFormat, new ObjectLogModel<D>( modelLocation, tmpPath ), id, tag, name, typeRef );
    }

    public AbstractObjectLogger( AbstractLoggerBackend backend, String timestampFormat, String resourceLocation, Path tmpPath, String id, String tag, String name, TypeRef<D> typeRef ) {
        this( backend, timestampFormat, new ObjectLogModel<D>( resourceLocation, tmpPath ), id, tag, name, typeRef );
    }

    private AbstractObjectLogger( AbstractLoggerBackend backend, String timestampFormat, ObjectLogModel<D> logModel, String id, String tag, String name, TypeRef<D> typeRef ) {
        super( backend, timestampFormat );
        this.name = name;
        this.renderer = logModel.renderer( typeRef, id, tag );
    }

    public void log( @Nonnull D data ) {
        this.log( prefix( data ), substitutions( data ), name, 0, renderer.headers(), renderer.render( data ) );
    }

    @Nonnull
    public abstract String prefix( @Nonnull D data );

    @Nonnull
    public abstract Map<String, String> substitutions( @Nonnull D data );

}
