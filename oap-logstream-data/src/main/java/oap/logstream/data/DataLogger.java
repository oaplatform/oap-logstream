/*
 *
 *  * Copyright (c) Xenoss
 *  * Unauthorized copying of this file, via any medium is strictly prohibited
 *  * Proprietary and confidential
 *
 *
 */

package oap.logstream.data;

import lombok.extern.slf4j.Slf4j;
import oap.logstream.Logger;
import oap.logstream.LoggerBackend;
import oap.util.AssocList;

import javax.annotation.Nonnull;
import java.util.Map;

@Slf4j
public class DataLogger extends Logger {
    private final Extractors extractors = new Extractors();

    public DataLogger( LoggerBackend backend ) {
        super( backend );
    }

    public DataLogger( LoggerBackend backend, String timestampFormat ) {
        super( backend, timestampFormat );
    }

    public void addExtractor( Extractor<?> extractor ) {
        this.extractors.add( extractor );
    }

    public <D> void log( String name, D data ) {
        @SuppressWarnings( "unchecked" ) Extractor<D> extractor = ( Extractor<D> ) extractors.get( name )
            .orElseThrow( () -> new IllegalStateException( "not extractor for " + name ) );
        log.trace( "name: {}, extractor: {}, data: {}, ", name, extractor, data );
        log( extractor.prefix( data ), extractor.substitutions( data ), name, 0, extractor.headers( data ), extractor.line( data ) );
    }

    public interface Extractor<D> {
        @Nonnull
        String prefix( @Nonnull D data );

        @Nonnull
        Map<String, String> substitutions( @Nonnull D data );

        @Nonnull
        String name();

        @Nonnull
        String headers( @Nonnull D data );

        @Nonnull
        String line( @Nonnull D data );
    }

    private static class Extractors extends AssocList<String, Extractor<?>> {
        @Override
        protected String keyOf( Extractor<?> extractor ) {
            return extractor.name();
        }
    }
}
