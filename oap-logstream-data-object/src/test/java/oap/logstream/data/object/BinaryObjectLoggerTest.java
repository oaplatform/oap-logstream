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

import oap.dictionary.DictionaryLeaf;
import oap.dictionary.DictionaryRoot;
import oap.dictionary.DictionaryValue;
import oap.logstream.LogId;
import oap.logstream.MemoryLoggerBackend;
import oap.net.Inet;
import oap.reflect.TypeRef;
import oap.template.BinaryUtils;
import oap.template.Types;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class BinaryObjectLoggerTest extends Fixtures {
    public BinaryObjectLoggerTest() {
        fixture( TestDirectoryFixture.FIXTURE );
    }

    @Test
    public void testLog() throws IOException {
        MemoryLoggerBackend memoryLoggerBackend = new MemoryLoggerBackend();
        BinaryObjectLogger binaryObjectLogger = new BinaryObjectLogger( new DictionaryRoot( "model", List.of(
            new DictionaryValue( "MODEL1", true, 1, List.of(
                new DictionaryLeaf( "a", true, 2, Map.of( "path", "a", "type", "STRING", "default", "" ) ),
                new DictionaryLeaf( "x", true, 2, Map.of( "type", "INTEGER", "default", 1 ) )
            ) )
        ) ), memoryLoggerBackend, TestDirectoryFixture.testPath( "tmp" ) );

        BinaryObjectLogger.TypedBinaryLogger<TestData> logger = binaryObjectLogger.typed( new TypeRef<>() {}, "MODEL1" );

        logger.log( new TestData( "ff" ), "prefix", Map.of(), "mylog", 0 );
        logger.log( new TestData( "kk" ), "prefix", Map.of(), "mylog", 0 );

        byte[] bytes = memoryLoggerBackend.loggedBytes( new LogId( "prefix", "mylog", Inet.HOSTNAME, 0, Map.of(),
            new String[] { "a" }, new byte[][] { new byte[] { Types.STRING.id } } ) );

        assertThat( BinaryUtils.read( bytes ) ).isEqualTo( List.of( List.of( "ff" ), List.of( "kk" ) ) );
    }

    public static class TestData {
        public String a;
        public int b;

        public TestData() {
        }

        public TestData( String a ) {
            this.a = a;
        }
    }
}
