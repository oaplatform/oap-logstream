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

package oap.logstream.disk;

import oap.logstream.Logger;
import oap.logstream.Timestamp;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import oap.util.Dates;
import org.testng.annotations.Test;

import java.util.Map;

import static oap.logstream.Timestamp.BPH_12;
import static oap.logstream.disk.DiskLoggerBackend.DEFAULT_BUFFER;
import static oap.net.Inet.HOSTNAME;
import static oap.testng.TestDirectoryFixture.testPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DiskLoggerBackendTest extends Fixtures {
    public DiskLoggerBackendTest() {
        fixture( TestDirectoryFixture.FIXTURE );
    }

    @Test
    public void spaceAvailable() {
        try( DiskLoggerBackend backend = new DiskLoggerBackend( testPath( "logs" ), Timestamp.BPH_12, 4000 ) ) {
            assertTrue( backend.isLoggingAvailable() );
            backend.requiredFreeSpace *= 1000;
            assertFalse( backend.isLoggingAvailable() );
            backend.requiredFreeSpace /= 1000;
            assertTrue( backend.isLoggingAvailable() );
        }
    }

    @Test
    public void testRefreshForceSync() {
        Dates.setTimeFixed( 2015, 10, 10, 1 );
        var headers = "REQUEST_ID\tREQUEST_ID2";
        var line = "12345678\t12345678";
        //init new logger
        try( DiskLoggerBackend backend = new DiskLoggerBackend( testPath( "logs" ), BPH_12, DEFAULT_BUFFER ) ) {
            Logger logger = new Logger( backend );
            //log a line to lfn1
            logger.log( "lfn1", Map.of(), "log", 1, headers, line );
            //check file size
            assertThat( testPath( "logs/lfn1/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
                .hasSize( 10 );
            //call refresh() with forceSync flag = true -> trigger flush()
            backend.refresh( true );
            //check file size once more after flush() -> now the size is larger
            assertThat( testPath( "logs/lfn1/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
                .hasSize( 74 );
        }
    }
}
