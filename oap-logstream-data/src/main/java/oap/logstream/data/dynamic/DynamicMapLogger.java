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

package oap.logstream.data.dynamic;

import lombok.extern.slf4j.Slf4j;
import oap.io.Resources;
import oap.logstream.Logger;
import oap.logstream.LoggerBackend;
import oap.logstream.data.map.MapLogModel;
import oap.logstream.data.map.MapLogRenderer;
import oap.util.AssocList;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Map;

@Slf4j
public class DynamicMapLogger extends Logger {
    private final Extractors extractors = new Extractors();

    public DynamicMapLogger( LoggerBackend backend ) {
        super( backend );
    }

    public DynamicMapLogger( LoggerBackend backend, String timestampFormat ) {
        super( backend, timestampFormat );
    }

    public void addExtractor( Extractor extractor ) {
        this.extractors.add( extractor );
    }

    public void log( String name, Map<String, Object> data ) {
        Extractor extractor = extractors.get( name )
            .orElseThrow( () -> new IllegalStateException( "not extractor for " + name ) );
        log.trace( "name: {}, extractor: {}, data: {}, ", name, extractor, data );
        log( extractor.prefix( data ), extractor.substitutions( data ), name, 0, extractor.renderer.headers(), extractor.renderer.render( data ) );
    }

    public abstract static class Extractor {
        private final MapLogRenderer renderer;

        public Extractor( Path modelLocation, String id, String tag ) {
            renderer = new MapLogModel( modelLocation ).renderer( id, tag );
        }

        public Extractor( String modelResource, String id, String tag ) {
            renderer = Resources.url( getClass(), modelResource )
                .map( url -> new MapLogModel( url ).renderer( id, tag ) )
                .orElseThrow( () -> new IllegalArgumentException( modelResource ) );
        }

        @Nonnull
        public abstract String prefix( @Nonnull Map<String, Object> data );

        @Nonnull
        public abstract Map<String, String> substitutions( @Nonnull Map<String, Object> data );

        @Nonnull
        public abstract String name();
    }

    private static class Extractors extends AssocList<String, Extractor> {
        @Override
        protected String keyOf( Extractor extractor ) {
            return extractor.name();
        }
    }
}
