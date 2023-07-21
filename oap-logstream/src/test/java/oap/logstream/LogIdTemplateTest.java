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

import oap.template.Types;
import oap.util.Dates;
import org.testng.annotations.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class LogIdTemplateTest {
    @Test
    public void testRender() {
        var h1Headers = new String[] { "h1" };
        var strTypes = new byte[][] { new byte[] { Types.STRING.id } };

        Dates.setTimeFixed( 2023, 1, 23, 21, 6, 0 );
        var lid1 = new LogId( "ln", "lt", "chn", 1, Map.of( "P", "PV" ), h1Headers, strTypes );

        LogIdTemplate logIdTemplate = new LogIdTemplate( lid1 );
        logIdTemplate.addVariable( "VVV", "123" );

        assertThat( logIdTemplate.render( "/<LOG_TYPE>/<LOG_VERSION>/<YEAR>/<LOG_TIME_INTERVAL>/<VVV>/<P>", Dates.nowUtc(), Timestamp.BPH_6, 4 ) )
            .isEqualTo( "/lt/85594397-4/2023/10/123/PV" );

        assertThat( logIdTemplate.render( "<UNKNOWN_VARIABLE>", Dates.nowUtc(), Timestamp.BPH_6, 4 ) )
            .isEqualTo( "" );
    }
}
