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

import oap.logstream.LogId;
import oap.logstream.LoggerBackend;
import oap.logstream.MemoryLoggerBackend;
import oap.net.Inet;
import oap.reflect.TypeRef;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Map;

import static oap.json.testng.JsonAsserts.objectOfTestJsonResource;
import static oap.testng.Asserts.assertString;
import static oap.testng.Asserts.pathOfTestResource;
import static org.assertj.core.api.Assertions.assertThat;

public class MapLoggerTest {
    @Test
    public void log() {
        MemoryLoggerBackend backend = new MemoryLoggerBackend();
        MapLogger logger = new EventMapLogger( backend, pathOfTestResource( getClass(), "datamodel.conf" ) );
        logger.log( objectOfTestJsonResource( getClass(), new TypeRef<Map<String, Object>>() {}.clazz(), "event.json" ) );
        assertThat( backend.logs() ).satisfies( m -> {
            LogId logId = new LogId( "/EVENT/${NAME}", "EVENT", Inet.HOSTNAME, 0, Map.of( "NAME", "event" ), "NAME\tVALUE1\tVALUE2\tVALUE3" );
            assertThat( m.keySet() ).containsOnly( logId );
            assertString( m.get( logId ) ).endsWith( "event\tvalue1\t222\t333\n" );
        } );
    }

    static class EventMapLogger extends MapLogger {
        public EventMapLogger( LoggerBackend backend, Path modelLocation ) {
            super( backend, new MapLogModel( modelLocation ), "EVENT", "LOG", "EVENT" );
        }

        @Nonnull
        @Override
        public String prefix( @Nonnull Map<String, Object> data ) {
            return "/EVENT/${NAME}";
        }

        @Nonnull
        @Override
        public Map<String, String> substitutions( @Nonnull Map<String, Object> data ) {
            return Map.of( "NAME", String.valueOf( data.get( "name" ) ) );
        }
    }
}
