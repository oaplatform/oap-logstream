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

import lombok.extern.slf4j.Slf4j;
import oap.logstream.AbstractLoggerBackend;
import oap.logstream.BackendLoggerNotAvailableException;
import oap.logstream.InvalidProtocolVersionException;
import oap.logstream.LogStreamProtocol;
import oap.logstream.LoggerException;
import oap.message.MessageListener;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedHashMap;

import static oap.logstream.LogStreamProtocol.MESSAGE_TYPE;

@Slf4j
public class SocketLoggerServer implements MessageListener, Closeable {
    private final AbstractLoggerBackend backend;

    public SocketLoggerServer( AbstractLoggerBackend backend ) {
        this.backend = backend;
    }

    @Override
    public byte getId() {
        return MESSAGE_TYPE;
    }

    @Override
    public String getInfo() {
        return "logstream";
    }

    @Override
    public short run( int version, String hostName, int size, byte[] data, String md5 ) {
        if( !backend.isLoggingAvailable() ) {
            var exception = new BackendLoggerNotAvailableException( hostName );
            backend.listeners.fireError( exception );
            return LogStreamProtocol.STATUS_BACKEND_LOGGER_NOT_AVAILABLE;
        }
        try( var in = new DataInputStream( new ByteArrayInputStream( data ) ) ) {
            switch( version ) {
                case 1 -> readOldTsv( version, hostName, in );
                case 2 -> readBinaryV1( version, hostName, in );
                default -> {
                    var exception = new InvalidProtocolVersionException( hostName, version );
                    backend.listeners.fireError( exception );
                    return LogStreamProtocol.INVALID_VERSION;
                }
            }
        } catch( EOFException e ) {
            var msg = "[" + hostName + "] " + " ended, closed";
            backend.listeners.fireWarning( msg );
            log.debug( msg );
            throw new LoggerException( e );
        } catch( LoggerException e ) {
            log.error( "[" + hostName + "] ", e );
            throw e;
        } catch( Exception e ) {
            backend.listeners.fireWarning( "[" + hostName + "] " );
            log.error( "[" + hostName + "] ", e );
            throw new LoggerException( e );
        }

        return LogStreamProtocol.STATUS_OK;
    }

    private void readBinaryV1( int version, String hostName, DataInputStream in ) throws IOException {
        in.readLong(); // digestion control
        var length = in.readInt();
        var filePreffix = in.readUTF();
        var logType = in.readUTF();
        var clientHostname = in.readUTF();
        int shard = in.readInt();

        int headersSize = in.readInt();
        var headers = new String[headersSize];
        for( var i = 0; i < headersSize; i++ ) {
            headers[i] = in.readUTF();
        }

        var types = new byte[headersSize][];
        for( var x = 0; x < headersSize; x++ ) {
            var tSize = in.readByte();
            var t = new byte[tSize];
            for( var y = 0; y < tSize; y++ ) {
                t[y] = in.readByte();
            }
            types[x] = t;
        }

        var propertiesSize = in.readByte();
        var properties = new LinkedHashMap<String, String>();
        for( var i = 0; i < propertiesSize; i++ ) {
            properties.put( in.readUTF(), in.readUTF() );
        }

        var buffer = new byte[length];
        in.readFully( buffer, 0, length );

        log.trace( "[{}] logging (properties {} filePreffix {} logType {} headers {} types {}, {})",
            hostName, properties, filePreffix, logType, headers, types, length );

        backend.log( version, clientHostname, filePreffix, properties, logType, shard, headers, types, buffer, 0, length );
    }

    private void readOldTsv( int version, String hostName, DataInputStream in ) throws IOException {
        in.readLong(); // digestion control
        var s = in.readInt();
        var filePreffix = in.readUTF();
        var logType = in.readUTF();
        var clientHostname = in.readUTF();
        int shard = in.readInt();
        var headers = in.readUTF();
        var propertiesSize = in.readByte();
        var properties = new LinkedHashMap<String, String>();
        for( var i = 0; i < propertiesSize; i++ ) {
            properties.put( in.readUTF(), in.readUTF() );
        }

        var buffer = new byte[s];
        in.readFully( buffer, 0, s );

        log.trace( "[{}] logging ({}/{}/{}/{}, {})", hostName, properties, filePreffix, logType, headers, s );

        backend.log( version, clientHostname, filePreffix, properties, logType, shard, new String[] { headers }, new byte[][] { { -1 } }, buffer, 0, s );
    }

    @Override
    public void close() {
//        @ToDo consider closing backend
//        backend.close();
    }
}
