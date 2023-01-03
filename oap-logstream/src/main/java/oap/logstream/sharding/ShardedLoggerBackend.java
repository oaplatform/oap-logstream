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

package oap.logstream.sharding;

import com.google.common.base.Preconditions;
import oap.logstream.AvailabilityReport;
import oap.logstream.AbstractLoggerBackend;
import oap.logstream.NoLoggerConfiguredForShardsException;
import oap.util.Stream;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static oap.util.Maps.Collectors.toMap;
import static oap.util.Pair.__;

public class ShardedLoggerBackend extends AbstractLoggerBackend {
    public final AbstractLoggerBackend[] loggers;

    public ShardedLoggerBackend( List<LoggerShardRange> shards ) throws NoLoggerConfiguredForShardsException {

        Preconditions.checkNotNull( shards );
        Preconditions.checkArgument( !shards.isEmpty() );

        shards.stream().mapToInt( l -> l.lower ).min().orElseThrow( () -> new IllegalArgumentException( "No logging ranges are configured (min)" ) );
        int maxShard = shards.stream().mapToInt( l -> l.upper ).max().orElseThrow( () -> new IllegalArgumentException( "No logging ranges are configured (max)" ) );

        loggers = new AbstractLoggerBackend[ maxShard + 1 ];

        for( LoggerShardRange ls : shards ) {
            for( int i = ls.lower; i <= ls.upper; i++ ) {
                loggers[i] = ls.backend;
            }
        }

        List<Integer> notConfiguredShards = Stream.of( loggers )
            .zipWithIndex()
            .filter( pair -> pair._1 == null )
            .map( pair -> pair._2 )
            .collect( Collectors.toList() );

        if( !notConfiguredShards.isEmpty() ) {
            var exception = new NoLoggerConfiguredForShardsException( notConfiguredShards );
            listeners.fireError( exception );
            throw exception;
        }
    }

    @Override
    public void log( String hostName, String filePreffix, Map<String, String> properties, String logType,
                     int shard, String headers, byte[] buffer, int offset, int length ) {
        loggers[shard].log( hostName, filePreffix, properties, logType, shard, headers, buffer, offset, length );
    }

    @Override
    public void close() {
        //NOOP because it's wrapper only
//        @ToDo consider closing all loggers beneath
    }

    @Override
    public AvailabilityReport availabilityReport() {
        Map<String, AvailabilityReport.State> reports = Stream.of( loggers )
            .distinct()
            .map( lb -> __( lb.toString(), lb.availabilityReport().state ) )
            .collect( toMap() );

        AvailabilityReport.State state = AvailabilityReport.State.PARTIALLY_OPERATIONAL;

        if( Stream.of( reports.values() ).allMatch( s -> s == AvailabilityReport.State.OPERATIONAL ) ) {
            state = AvailabilityReport.State.OPERATIONAL;
        } else if( Stream.of( reports.values() ).allMatch( s -> s == AvailabilityReport.State.FAILED ) ) {
            state = AvailabilityReport.State.FAILED;
        }

        return new AvailabilityReport( state, reports );
    }

    @Override
    public boolean isLoggingAvailable() {
        return Stream.of( loggers ).allMatch( AbstractLoggerBackend::isLoggingAvailable );
    }
}
