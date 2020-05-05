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

import oap.io.IoStreams.Encoding;
import oap.logstream.disk.DiskLoggerBackend;
import oap.logstream.net.SocketLoggerBackend;
import oap.logstream.net.SocketLoggerServer;
import oap.message.MessageSender;
import oap.message.MessageServer;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import oap.util.Dates;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static oap.logstream.Timestamp.BPH_12;
import static oap.logstream.disk.DiskLoggerBackend.DEFAULT_BUFFER;
import static oap.logstream.disk.DiskLoggerBackend.DEFAULT_FREE_SPACE_REQUIRED;
import static oap.net.Inet.HOSTNAME;
import static oap.testng.Asserts.assertFile;
import static oap.testng.TestDirectoryFixture.testPath;
import static oap.util.Dates.formatDateWithMillis;
import static org.joda.time.DateTimeUtils.currentTimeMillis;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LoggerTest extends Fixtures {
    {
        fixture( TestDirectoryFixture.FIXTURE );
    }

    @Test
    public void disk() {
        Dates.setTimeFixed( 2015, 10, 10, 1, 0 );

        var content = "12345678";
        var headers = "DATETIME\tREQUEST_ID\tREQUEST_ID2";
        var contentWithHeaders = headers + "\n" + formatDateWithMillis( currentTimeMillis() ) + "\t12345678";
        var headers2 = "DATETIME\tREQUEST_ID2";
        var content2WithHeaders = headers2 + "\n" + formatDateWithMillis( currentTimeMillis() ) + "\t12345678";
        try( DiskLoggerBackend backend = new DiskLoggerBackend( testPath( "logs" ), BPH_12, DEFAULT_BUFFER ) ) {
            Logger logger = new Logger( backend );
            logger.log( "lfn1", Map.of(), "log", 1, headers, content );
            logger.log( "lfn2", Map.of(), "log", 1, headers, content );
            logger.log( "lfn1", Map.of(), "log", 1, headers, content );
            logger.log( "lfn1", Map.of(), "log2", 1, headers2, content );
        }

        assertFile( testPath( "logs/lfn1/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( contentWithHeaders + "\n"
                + formatDateWithMillis( currentTimeMillis() ) + "\t" + content + "\n", Encoding.GZIP );
        assertFile( testPath( "logs/lfn2/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( contentWithHeaders + "\n", Encoding.GZIP );
        assertFile( testPath( "logs/lfn1/2015-10/10/log2_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( content2WithHeaders + "\n", Encoding.GZIP );
    }

    @Test
    public void net() {
        Dates.setTimeFixed( 2015, 10, 10, 1, 0 );

        var content = "12345678";
        var headers = "DATETIME\tREQUEST_ID\tREQUEST_ID2";
        var contentWithHeaders = headers + "\n" + formatDateWithMillis( currentTimeMillis() ) + "\t12345678";
        var headers2 = "DATETIME\tREQUEST_ID2";
        var content2WithHeaders = headers2 + "\n" + formatDateWithMillis( currentTimeMillis() ) + "\t12345678";

        try( var serverBackend = new DiskLoggerBackend( testPath( "logs" ), BPH_12, DEFAULT_BUFFER );
             var server = new SocketLoggerServer( serverBackend );
             var mserver = new MessageServer( testPath( "controlStatePath.st" ), 0, List.of( server ), -1 ) ) {
            mserver.start();

            try( var mclient = new MessageSender( "localhost", mserver.getPort(), testPath( "tmp" ) );
                 var clientBackend = new SocketLoggerBackend( mclient, 256, -1 ) ) {
                mclient.start();

                serverBackend.requiredFreeSpace = DEFAULT_FREE_SPACE_REQUIRED * 10000L;
                assertFalse( serverBackend.isLoggingAvailable() );
                var logger = new Logger( clientBackend );
                logger.log( "lfn1", Map.of(), "log", 1, headers, content );
                clientBackend.send();
                assertTrue( logger.isLoggingAvailable() );

                clientBackend.send();
                assertTrue( logger.isLoggingAvailable() );

                assertFile( testPath( "logs/lfn1/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
                    .doesNotExist();

                serverBackend.requiredFreeSpace = DEFAULT_FREE_SPACE_REQUIRED;
                assertTrue( serverBackend.isLoggingAvailable() );
                clientBackend.send();
                assertTrue( logger.isLoggingAvailable() );

                logger.log( "lfn2", Map.of(), "log", 1, headers, content );
                logger.log( "lfn1", Map.of(), "log", 1, headers, content );
                logger.log( "lfn1", Map.of(), "log2", 1, headers2, content );
                clientBackend.send();
            }
        }

        assertFile( testPath( "logs/lfn1/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( contentWithHeaders + "\n"
                + formatDateWithMillis( currentTimeMillis() ) + "\t" + content + "\n", Encoding.GZIP );
        assertFile( testPath( "logs/lfn2/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( contentWithHeaders + "\n", Encoding.GZIP );
        assertFile( testPath( "logs/lfn1/2015-10/10/log2_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( content2WithHeaders + "\n", Encoding.GZIP );
    }
}
