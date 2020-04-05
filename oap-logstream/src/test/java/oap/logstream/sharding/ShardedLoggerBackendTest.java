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

import oap.logstream.AvailabilityReport;
import oap.logstream.LoggerBackend;
import oap.logstream.NoLoggerConfiguredForShardsException;
import oap.util.Stream;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static oap.logstream.AvailabilityReport.State.FAILED;
import static oap.logstream.AvailabilityReport.State.OPERATIONAL;
import static oap.logstream.AvailabilityReport.State.PARTIALLY_OPERATIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ShardedLoggerBackendTest {
    @Test
    public void routing() {
        var log1 = mock( LoggerBackend.class );
        var log2 = mock( LoggerBackend.class );

        var shard0To100 = new LoggerShardRange( log1, 0, 100 );
        var shard100To200 = new LoggerShardRange( log2, 100, 200 );

        var shards = List.of( shard0To100, shard100To200 );
        var slb = new ShardedLoggerBackend( shards );

        slb.log( "localhost", "", Map.of( "f", "1" ), "t1", 34, "h1", "line1" );
        slb.log( "localhost", "", Map.of( "f", "2" ), "t1", 142, "h1", "line2" );

        verify( log1 ).log( "localhost", "", Map.of( "f", "1" ), "t1", 34, "h1", "line1\n".getBytes(), 0, "line1\n".getBytes().length );
        verify( log2 ).log( "localhost", "", Map.of( "f", "2" ), "t1", 142, "h1", "line2\n".getBytes(), 0, "line2\n".getBytes().length );
    }

    @Test( expectedExceptions = NoLoggerConfiguredForShardsException.class )
    public void unconfiguredShards() {
        var log1 = mock( LoggerBackend.class );

        var shard0To100 = new LoggerShardRange( log1, 0, 100 );
        var shard100To200 = new LoggerShardRange( log1, 110, 200 );

        var shards = List.of( shard0To100, shard100To200 );
        new ShardedLoggerBackend( shards );
    }

    @Test
    public void availability() {
        var log1 = mock( LoggerBackend.class );
        var log2 = mock( LoggerBackend.class );

        var shard0To100 = new LoggerShardRange( log1, 0, 100 );
        var shard100To200 = new LoggerShardRange( log2, 100, 200 );

        var shards = List.of( shard0To100, shard100To200 );
        var slb = new ShardedLoggerBackend( shards );


        when( log1.availabilityReport() ).thenReturn( new AvailabilityReport( OPERATIONAL ) );
        when( log2.availabilityReport() ).thenReturn( new AvailabilityReport( OPERATIONAL ) );
        assertEquals( slb.availabilityReport().state, OPERATIONAL );
        assertEquals( slb.availabilityReport().subsystemStates.size(), 2 );
        assertTrue( Stream.of( slb.availabilityReport().subsystemStates.values() ).allMatch( s -> s == OPERATIONAL ) );

        when( log1.availabilityReport() ).thenReturn( new AvailabilityReport( FAILED ) );
        when( log2.availabilityReport() ).thenReturn( new AvailabilityReport( FAILED ) );
        assertEquals( slb.availabilityReport().state, FAILED );
        assertEquals( slb.availabilityReport().subsystemStates.size(), 2 );
        assertTrue( Stream.of( slb.availabilityReport().subsystemStates.values() ).allMatch( s -> s == FAILED ) );

        when( log1.availabilityReport() ).thenReturn( new AvailabilityReport( OPERATIONAL ) );
        when( log2.availabilityReport() ).thenReturn( new AvailabilityReport( FAILED ) );
        assertEquals( slb.availabilityReport().state, PARTIALLY_OPERATIONAL );
        assertEquals( slb.availabilityReport().subsystemStates.size(), 2 );
        assertThat( slb.availabilityReport().subsystemStates ).containsValue( OPERATIONAL );
        assertThat( slb.availabilityReport().subsystemStates ).containsValue( FAILED );
    }
}
