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

import oap.net.Inet;

import java.util.Map;

public class Logger {
    public static final String DEFAULT_TIMESTAMP = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DEFAULT_TIMESTAMP_NAME = "TIMESTAMP";
    private final AbstractLoggerBackend backend;
    private final DateTimeFormatter formatter;

    public Logger( AbstractLoggerBackend backend ) {
        this.backend = backend;
        this.formatter = DateTimeFormat.forPattern( timestampFormat ).withZoneUTC();
    }

    public void log( String filePreffix, Map<String, String> properties, String logType, int shard, String headers, String line ) {
        backend.log( Inet.HOSTNAME, filePreffix, properties, logType, shard, DEFAULT_TIMESTAMP_NAME + "\t" + headers, formatter.print( DateTimeUtils.currentTimeMillis() ) + "\t" + line );
    }

    public void log( String filePreffix, Map<String, String> properties, String logType, int shard,
                     String[] headers, byte[][] types, byte[] row ) {
        backend.log( Inet.HOSTNAME, filePreffix, properties, logType, shard, headers, types, row );
    }

    public boolean isLoggingAvailable() {
        return backend.isLoggingAvailable();
    }

    public AvailabilityReport availabilityReport() {
        return backend.availabilityReport();
    }
}
