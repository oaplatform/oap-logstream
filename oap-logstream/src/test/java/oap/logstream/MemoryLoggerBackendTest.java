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

package oap.logstream;

import org.testng.annotations.Test;

import java.util.Map;

import static oap.testng.Asserts.assertString;
import static org.assertj.core.api.Assertions.assertThat;

public class MemoryLoggerBackendTest {
    @Test
    public void lines() {
        MemoryLoggerBackend backend = new MemoryLoggerBackend();
        backend.log( "test1", "file1", Map.of(), "type1", 1, "h1", "line1" );
        backend.log( "test1", "file1", Map.of(), "type1", 1, "h1", "line2" );

        assertThat( backend.loggedLines( new LogId( "file1", "type1", "test1", 1, Map.of(), "h1" ) ) )
            .containsExactly( "line1", "line2" );

        assertString( backend.logged( new LogId( "file1", "type1", "test1", 1, Map.of(), "h1" ) ) )
            .isEqualTo( "line1\nline2\n" );

        assertString( backend.logged() )
            .isEqualTo( "line1\nline2\n" );
    }

}
