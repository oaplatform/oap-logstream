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

package oap.logstream.data;

import oap.logstream.MemoryLoggerBackend;
import oap.logstream.data.map.MapDataModel;
import oap.logstream.data.map.MapDataModelExtractor;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Map;

import static oap.json.testng.JsonAsserts.objectOfTestJsonResource;
import static oap.testng.Asserts.assertString;
import static oap.testng.Asserts.pathOfTestResource;

public class DataLoggerTest {

    @Test
    public void log() {
        MemoryLoggerBackend backend = new MemoryLoggerBackend();
        DataLogger logger = new DataLogger( backend );
        logger.addExtractor( new TestExtractor( new MapDataModel( pathOfTestResource( getClass(), "datamodel.conf" ) ) ) );
        logger.log( "EVENT", objectOfTestJsonResource( getClass(), Map.class, "event.json" ) );
        assertString( backend.logged() ).endsWith( "event\tvalue1\t222\t333\n" );
    }

    public static class TestExtractor extends MapDataModelExtractor {
        public static final String ID = "EVENT";

        public TestExtractor( MapDataModel dataModel ) {
            super( dataModel, ID, "LOG" );
        }

        @Override
        @Nonnull
        public String prefix( @Nonnull Map<String, Object> data ) {
            return "/event1/${NAME}";
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
