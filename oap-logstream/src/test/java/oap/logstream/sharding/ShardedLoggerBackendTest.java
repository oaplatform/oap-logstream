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
import oap.logstream.MemoryLoggerBackend;
import oap.logstream.NoLoggerConfiguredForShardsException;
import oap.template.Types;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static oap.logstream.AvailabilityReport.State.FAILED;
import static oap.logstream.AvailabilityReport.State.OPERATIONAL;
import static oap.logstream.AvailabilityReport.State.PARTIALLY_OPERATIONAL;
import static oap.logstream.LogStreamProtocol.CURRENT_PROTOCOL_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

public class ShardedLoggerBackendTest {
    @Test
    public void routing() {
        var log1 = new MemoryLoggerBackend();
        var log2 = new MemoryLoggerBackend();

        var shard0To100 = new LoggerShardRange( log1, 0, 100 );
        var shard100To200 = new LoggerShardRange( log2, 100, 200 );

        var shards = List.of( shard0To100, shard100To200 );
        var slb = new ShardedLoggerBackend( shards );

        slb.log( CURRENT_PROTOCOL_VERSION, "localhost", "", Map.of( "f", "1" ), "t1", 34,
            new String[] { "h1" }, new byte[][] { new byte[] { Types.STRING.id } }, "line1".getBytes( UTF_8 ) );
        slb.log( CURRENT_PROTOCOL_VERSION, "localhost", "", Map.of( "f", "2" ), "t1", 142,
            new String[] { "h1" }, new byte[][] { new byte[] { Types.STRING.id } }, "line2".getBytes( UTF_8 ) );
        assertThat( log1.loggedLines() ).containsExactly( "line1" );
        assertThat( log2.loggedLines() ).containsExactly( "line2" );
    }

    @Test( expectedExceptions = NoLoggerConfiguredForShardsException.class )
    public void unconfiguredShards() {
        var log1 = new MemoryLoggerBackend();

        var shard0To100 = new LoggerShardRange( log1, 0, 100 );
        var shard100To200 = new LoggerShardRange( log1, 110, 200 );

        var shards = List.of( shard0To100, shard100To200 );
        new ShardedLoggerBackend( shards );
    }

    @Test
    public void availability() {
        var log1 = new TestBackend();
        var log2 = new TestBackend();

        var shard0To100 = new LoggerShardRange( log1, 0, 100 );
        var shard100To200 = new LoggerShardRange( log2, 100, 200 );

        var shards = List.of( shard0To100, shard100To200 );
        var slb = new ShardedLoggerBackend( shards );


        assertThat( slb.availabilityReport().state ).isEqualTo( OPERATIONAL );
        assertThat( slb.availabilityReport().subsystemStates ).hasSize( 2 );
        assertThat( slb.availabilityReport().subsystemStates.values() ).allMatch( s -> s == OPERATIONAL );


        log1.state = FAILED;
        log2.state = FAILED;
        assertThat( slb.availabilityReport().state ).isEqualTo( FAILED );
        assertThat( slb.availabilityReport().subsystemStates ).hasSize( 2 );
        assertThat( slb.availabilityReport().subsystemStates.values() ).allMatch( s -> s == FAILED );

        log1.state = OPERATIONAL;
        log2.state = FAILED;
        assertThat( slb.availabilityReport().state ).isEqualTo( PARTIALLY_OPERATIONAL );
        assertThat( slb.availabilityReport().subsystemStates ).hasSize( 2 );
        assertThat( slb.availabilityReport().subsystemStates ).containsValue( OPERATIONAL );
        assertThat( slb.availabilityReport().subsystemStates ).containsValue( FAILED );
    }

    static class TestBackend extends MemoryLoggerBackend {
        AvailabilityReport.State state = OPERATIONAL;

        @Override
        public AvailabilityReport availabilityReport() {
            return new AvailabilityReport( state );
        }
    }
}
