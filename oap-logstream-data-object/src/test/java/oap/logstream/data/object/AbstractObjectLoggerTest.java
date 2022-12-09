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
import oap.logstream.LogId;
import oap.logstream.MemoryLoggerBackend;
import oap.net.Inet;
import oap.reflect.TypeRef;
import oap.testng.Fixtures;
import oap.testng.SystemTimerFixture;
import oap.testng.TestDirectoryFixture;
import oap.util.Dates;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Map;

import static oap.testng.Asserts.objectOfTestResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class AbstractObjectLoggerTest extends Fixtures {
    public AbstractObjectLoggerTest() {
        fixture( SystemTimerFixture.FIXTURE );
    }

    @Test
    public void log() {
        Dates.setTimeFixed( 2021, 1, 1, 1 );
        MemoryLoggerBackend backend = new MemoryLoggerBackend();
        EventObjectLogger logger = new EventObjectLogger( backend, objectOfTestResource( DictionaryRoot.class, getClass(), "datamodel.conf" ), TestDirectoryFixture.testDirectory() );
        logger.log( new Event( "event", "value1", 222, 333 ) );
        assertThat( backend.logs() ).containsExactly( entry(
            new LogId( "/EVENT/${NAME}", "EVENT", "EVENT", Inet.HOSTNAME, 0, Map.of( "NAME", "event" ), "TIMESTAMP\tNAME\tVALUE1\tVALUE2\tVALUE3" ),
            "2021-01-01 01:00:00.000\tevent\tvalue1\t222\t333\n"
        ) );
    }

    static class EventObjectLogger extends AbstractObjectLogger<Event> {

        EventObjectLogger( AbstractLoggerBackend backend, DictionaryRoot model, Path tmpPath ) {
            super( backend, model, tmpPath, "EVENT", "LOG", "EVENT", new TypeRef<>() {} );
        }

        @Nonnull
        @Override
        public String prefix( @Nonnull Event data ) {
            return "/EVENT/${NAME}";
        }

        @Nonnull
        @Override
        public Map<String, String> substitutions( @Nonnull Event data ) {
            return Map.of( "NAME", data.name );
        }
    }

    public static class Event {
        public String name;
        public String value1;
        public int value2;
        public Obj obj;

        public Event( String name, String value1, int value2, int value3 ) {
            this.name = name;
            this.value1 = value1;
            this.value2 = value2;
            this.obj = new Obj( value3 );
        }

        public static class Obj {
            public int value3;

            public Obj( int value3 ) {
                this.value3 = value3;
            }
        }
    }
}
