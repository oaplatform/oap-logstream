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

import oap.dictionary.DictionaryRoot;
import oap.dictionary.DictionaryValue;
import oap.logstream.LogId;
import oap.logstream.MemoryLoggerBackend;
import oap.reflect.TypeRef;
import oap.testng.Fixtures;
import oap.testng.SystemTimerFixture;
import oap.util.Dates;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import static oap.json.testng.JsonAsserts.objectOfTestJsonResource;
import static oap.net.Inet.HOSTNAME;
import static oap.testng.Asserts.objectOfTestResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class DynamicMapLoggerTest extends Fixtures {

    {
        fixture( SystemTimerFixture.FIXTURE );
    }

    @Test
    public void log() {
        Dates.setTimeFixed( 2021, 1, 1, 1 );
        MemoryLoggerBackend backend = new MemoryLoggerBackend();
        DictionaryRoot datamodel = new DictionaryRoot( "root", List.of( new DictionaryValue( "EVENT", true, 1 ) ) );
        DynamicMapLogger logger = new DynamicMapLogger( backend, datamodel );
        logger.addExtractor( new TestExtractor( objectOfTestResource( DictionaryRoot.class, getClass(), "datamodel.conf" ) ) );
        logger.log( "EVENT", objectOfTestJsonResource( getClass(), new TypeRef<Map<String, Object>>() {}.clazz(), "event.json" ) );
        assertThat( backend.logs() ).containsExactly( entry(
            new LogId( "/EVENT/${NAME}", "EVENT", "EVENT", HOSTNAME, 0, Map.of( "NAME", "event" ), "TIMESTAMP\tNAME\tVALUE1\tVALUE2\tVALUE3" ),
            "2021-01-01 01:00:00.000\tevent\tvalue1\t222\t333\n"
        ) );
    }

    public static class TestExtractor extends DynamicMapLogger.AbstractExtractor {
        public static final String ID = "EVENT";

        public TestExtractor( DictionaryRoot model ) {
            super( model, ID, "LOG" );
        }

        @Override
        @Nonnull
        public String prefix( @Nonnull Map<String, Object> data ) {
            return "/EVENT/${NAME}";
        }

        @Nonnull
        @Override
        public Map<String, String> substitutions( @Nonnull Map<String, Object> data ) {
            return Map.of( "NAME", String.valueOf( data.get( "name" ) ) );
        }

        @Override
        @Nonnull
        public String name() {
            return ID;
        }

    }
}
