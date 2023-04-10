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
package oap.logstream.net;

import io.micrometer.core.instrument.Metrics;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.logstream.LogId;
import oap.logstream.net.BufferConfigurationMap.BufferConfiguration;
import oap.util.Cuid;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

@EqualsAndHashCode( exclude = "closed" )
@ToString
@Slf4j
public class Buffers implements Closeable {
    static Cuid digestionIds = Cuid.UNIQUE;

    //    private final int bufferSize;
    private final ConcurrentHashMap<String, Buffer> currentBuffers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<LogId, BufferConfiguration> configurationForSelector = new ConcurrentHashMap<>();
    private final BufferConfigurationMap configurations;
    private final ReadyBuffers readyBuffers;
    BufferCache cache;
    private volatile boolean closed;

    public Buffers( BufferConfigurationMap configurations, ReadyBuffers readyBuffers ) {
        this.configurations = configurations;
        this.cache = new BufferCache();
        this.readyBuffers = buffer -> {
            buffer.close( digestionIds.nextLong() );
            readyBuffers.ready( buffer );
        };
    }

    public final void put( LogId key, byte[] buffer ) {
        put( key, buffer, 0, buffer.length );
    }

    public final void put( LogId id, byte[] buffer, int offset, int length ) {
        if( closed ) throw new IllegalStateException( "current buffer is already closed" );

        var conf = configurationForSelector.computeIfAbsent( id, this::findConfiguration );

        var bufferSize = conf.bufferSize;
        var intern = id.lock();
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized( intern ) {
            var b = currentBuffers.computeIfAbsent( intern, k -> cache.get( id, bufferSize ) );
            if( bufferSize - b.headerLength() < length )
                throw new IllegalArgumentException( "buffer size is too big: " + length + " for buffer of " + bufferSize + "; headers = " + b.headerLength() );
            if( !b.available( length ) ) {
                readyBuffers.ready( b );
                currentBuffers.put( intern, b = cache.get( id, bufferSize ) );
            }
            b.put( buffer, offset, length );
        }
    }

    private BufferConfiguration findConfiguration( LogId id ) {
        for( var conf : configurations.entrySet() ) {
            if( conf.getValue().pattern.matcher( id.logType ).find() ) return conf.getValue();
        }
        throw new IllegalStateException( "Pattern for " + id + " not found" );
    }

    public void flush() {
        for( var internSelector : currentBuffers.keySet() ) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized( internSelector ) {
                var buffer = currentBuffers.remove( internSelector );
                if( buffer != null && !buffer.isEmpty() ) readyBuffers.ready( buffer );
            }
        }

    }

    @Override
    public final synchronized void close() {
        if( closed ) throw new IllegalStateException( "already closed" );
        flush();
        closed = true;
    }

    public void report() {
        var buffers = new ArrayList<>( currentBuffers.values() );

        var map = new HashMap<String, MutableLong>();
        for( var buffer : buffers ) {
            var logType = buffer.id.logType;
            map.computeIfAbsent( logType, lt -> new MutableLong() ).increment();
        }

        map.forEach( ( type, count ) -> Metrics.summary( "logstream_logging_buffers", "type", type ).record( count.getValue() ) );
    }

    public static class BufferCache {
        private final Map<Integer, Queue<Buffer>> cache = new HashMap<>();

        private synchronized Buffer get( LogId id, int bufferSize ) {
            var list = cache.computeIfAbsent( bufferSize, bs -> new LinkedList<>() );

            if( list.isEmpty() ) return new Buffer( bufferSize, id );
            else {
                var buffer = list.poll();
                buffer.reset( id );
                return buffer;
            }
        }

        private synchronized void release( Buffer buffer ) {
            var list = cache.get( buffer.length() );
            if( list != null ) list.offer( buffer );
        }

        public final int size( int bufferSize ) {
            var list = cache.get( bufferSize );
            return list != null ? list.size() : 0;
        }
    }

    public interface ReadyBuffers {
        void ready( Buffer buffer );
    }
}
