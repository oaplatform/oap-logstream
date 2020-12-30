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

import lombok.extern.slf4j.Slf4j;
import oap.io.IoStreams.Encoding;
import oap.logstream.disk.DiskLoggerBackend;
import oap.logstream.net.SocketLoggerBackend;
import oap.logstream.net.SocketLoggerServer;
import oap.message.MessageSender;
import oap.message.MessageServer;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import oap.util.Dates;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static oap.logstream.Timestamp.BPH_12;
import static oap.logstream.disk.DiskLoggerBackend.DEFAULT_BUFFER;
import static oap.logstream.disk.DiskLoggerBackend.DEFAULT_FREE_SPACE_REQUIRED;
import static oap.net.Inet.HOSTNAME;
import static oap.testng.Asserts.assertEventually;
import static oap.testng.Asserts.assertFile;
import static oap.testng.TestDirectoryFixture.testPath;
import static org.joda.time.DateTimeUtils.currentTimeMillis;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Slf4j
public class LoggerTest extends Fixtures {
    DateTimeFormatter formatter = DateTimeFormat.forPattern( Logger.DEFAULT_TIMESTAMP).withZoneUTC();
    
    {
        fixture( TestDirectoryFixture.FIXTURE );
    }

    @Test
    public void disk() {
        Dates.setTimeFixed( 2015, 10, 10, 1, 0 );

        var content = "12345678";
        var headers = "DATETIME\tREQUEST_ID\tREQUEST_ID2";
        var contentWithHeaders = headers + "\n" + formatter.print( currentTimeMillis() ) + "\t12345678";
        var headers2 = "DATETIME\tREQUEST_ID2";
        var content2WithHeaders = headers2 + "\n" + formatter.print( currentTimeMillis() ) + "\t12345678";
        try( DiskLoggerBackend backend = new DiskLoggerBackend( testPath( "logs" ), BPH_12, DEFAULT_BUFFER ) ) {
            Logger logger = new Logger( backend );
            logger.log( "lfn1", Map.of(), "log", 1, headers, content );
            logger.log( "lfn2", Map.of(), "log", 1, headers, content );
            logger.log( "lfn1", Map.of(), "log", 1, headers, content );
            logger.log( "lfn1", Map.of(), "log2", 1, headers2, content );

            logger.log( "lfn1", Map.of(), "log", 1, headers2, content );
        }

        assertFile( testPath( "logs/lfn1/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( contentWithHeaders + "\n"
                + formatter.print( currentTimeMillis() ) + "\t" + content + "\n", Encoding.GZIP );
        assertFile( testPath( "logs/lfn2/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( contentWithHeaders + "\n", Encoding.GZIP );
        assertFile( testPath( "logs/lfn1/2015-10/10/log2_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( content2WithHeaders + "\n", Encoding.GZIP );

        assertFile( testPath( "logs/lfn1/2015-10/10/log_v2_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( content2WithHeaders + "\n", Encoding.GZIP );
    }

    @Test
    public void net() throws InterruptedException, ExecutionException, TimeoutException {
        Dates.setTimeFixed( 2015, 10, 10, 1, 0 );

        var content = "12345678";
        var headers = "DATETIME\tREQUEST_ID\tREQUEST_ID2";
        var contentWithHeaders = headers + "\n" + formatter.print( currentTimeMillis() ) + "\t12345678";
        var headers2 = "DATETIME\tREQUEST_ID2";
        var content2WithHeaders = headers2 + "\n" + formatter.print( currentTimeMillis() ) + "\t12345678";

        try( var serverBackend = new DiskLoggerBackend( testPath( "logs" ), BPH_12, DEFAULT_BUFFER );
             var server = new SocketLoggerServer( serverBackend );
             var mserver = new MessageServer( testPath( "controlStatePath.st" ), 0, List.of( server ), -1 ) ) {
            mserver.soTimeout = ( int ) Dates.s( 1 );
            mserver.start();

            try( var mclient = new MessageSender( "localhost", mserver.getPort(), testPath( "tmp" ) );
                 var clientBackend = new SocketLoggerBackend( mclient, 256, -1 ) ) {
                mclient.start();

                serverBackend.requiredFreeSpace = DEFAULT_FREE_SPACE_REQUIRED * 10000L;
                assertFalse( serverBackend.isLoggingAvailable() );
                var logger = new Logger( clientBackend );
                logger.log( "lfn1", Map.of(), "log", 1, headers, content );
                var f = CompletableFuture.runAsync( clientBackend::send );
                assertTrue( logger.isLoggingAvailable() );
                assertFalse( f.isDone() );

                assertTrue( logger.isLoggingAvailable() );
                assertFalse( f.isDone() );

                assertFile( testPath( "logs/lfn1/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
                    .doesNotExist();

                serverBackend.requiredFreeSpace = DEFAULT_FREE_SPACE_REQUIRED;

                log.debug( "add disk space" );

                assertTrue( serverBackend.isLoggingAvailable() );
                f.get( Dates.s( 10 ), TimeUnit.MILLISECONDS );

                assertTrue( logger.isLoggingAvailable() );
                logger.log( "lfn2", Map.of(), "log", 1, headers, content );

                assertTrue( logger.isLoggingAvailable() );
                logger.log( "lfn1", Map.of(), "log", 1, headers, content );

                assertTrue( logger.isLoggingAvailable() );
                logger.log( "lfn1", Map.of(), "log2", 1, headers2, content );

                clientBackend.send();
            }
        }

        assertEventually( 100, 1000, () ->
            assertFile( testPath( "logs/lfn1/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
                .hasContent( contentWithHeaders + "\n"
                    + formatter.print( currentTimeMillis() ) + "\t" + content + "\n", Encoding.GZIP ) );
        assertFile( testPath( "logs/lfn2/2015-10/10/log_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( contentWithHeaders + "\n", Encoding.GZIP );
        assertFile( testPath( "logs/lfn1/2015-10/10/log2_v1_" + HOSTNAME + "-2015-10-10-01-00.tsv.gz" ) )
            .hasContent( content2WithHeaders + "\n", Encoding.GZIP );
    }
}
