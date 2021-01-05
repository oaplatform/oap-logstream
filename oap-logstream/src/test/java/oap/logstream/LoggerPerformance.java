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

import ch.qos.logback.classic.LoggerContext;
import oap.benchmark.Benchmark;
import oap.logstream.net.SocketLoggerBackend;
import oap.logstream.net.SocketLoggerServer;
import oap.message.MessageSender;
import oap.message.MessageServer;
import oap.testng.Env;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import oap.util.Try;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static oap.testng.TestDirectoryFixture.testPath;
import static oap.util.Dates.formatDateWithMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeUtils.currentTimeMillis;

public class LoggerPerformance extends Fixtures {
    private final static int SAMPLES = 1000000;
    private final static int EXPERIMENTS = 5;
    private final static int THREADS = 500;

    {
        fixture( TestDirectoryFixture.FIXTURE );
    }

    @Test
    public void net() {
        LoggerContext loggerContext = ( LoggerContext ) LoggerFactory.getILoggerFactory();
        try {
            loggerContext.stop();
            var headers = "DATETIME\tREQUEST_ID";

            var loggingBacked = new PerfLoggingBacked();
            var port = Env.port( "st" );
            try( var server = new SocketLoggerServer( loggingBacked );
                 var mserver = new MessageServer( testPath( "controlStatePath.st" ), port, List.of( server ), -1 );
                 var mclient = new MessageSender( "localhost", port, testPath( "tmp" ) );
                 var clientBackend = new SocketLoggerBackend( mclient, 1024, 10 ) ) {
                var logger = new Logger( clientBackend );

                clientBackend.maxBuffers = 1024;
                mclient.poolSize = 64;
                mserver.start();
                mclient.start();

                Benchmark.benchmark( "logstream", SAMPLES, () -> {
                    while( !logger.isLoggingAvailable() ) {
                        Thread.sleep( 1 );
                    }
                    logger.log( "lfn1", Map.of( "a", "v" ), "log", 1, headers, formatDateWithMillis( currentTimeMillis() ) + "\tVALUE" );
                } ).inThreads( THREADS, SAMPLES )
                    .experiments( EXPERIMENTS )
                    .afterExperiment( new Try.CatchingRunnable( clientBackend::sendAsync ) )
                    .run();


                assertThat( loggingBacked.count.get() ).isEqualTo( SAMPLES * EXPERIMENTS + SAMPLES );
            }

        } finally {
            loggerContext.start();
        }
    }

    public static class PerfLoggingBacked extends LoggerBackend {

        public final AtomicLong count = new AtomicLong();

        @Override
        public void log( String hostName, String filePreffix, Map<String, String> properties, String logType, int shard, String headers, byte[] buffer, int offset, int length ) {
            var data = new String( buffer, offset, length, StandardCharsets.UTF_8 );
            count.addAndGet( StringUtils.countMatches( data, "VALUE" ) );
        }

        @Override
        public void close() {
        }

        @Override
        public AvailabilityReport availabilityReport() {
            return new AvailabilityReport( AvailabilityReport.State.OPERATIONAL );
        }
    }
}
