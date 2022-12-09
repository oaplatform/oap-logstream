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

import oap.dictionary.DictionaryRoot;
import oap.net.Inet;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashSet;
import java.util.Map;

public class Logger {
    public static final String DEFAULT_TIMESTAMP = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DEFAULT_TIMESTAMP_NAME = "TIMESTAMP";
    private final AbstractLoggerBackend backend;
    private final DateTimeFormatter formatter;
    private final HashSet<String> dataTypes;
    private String timestampName = DEFAULT_TIMESTAMP_NAME;

    public Logger( AbstractLoggerBackend backend, DictionaryRoot datamodel ) {
        this( backend, datamodel, DEFAULT_TIMESTAMP );
    }

    public Logger( AbstractLoggerBackend backend, DictionaryRoot datamodel, String timestampFormat ) {
        this.backend = backend;
        this.formatter = DateTimeFormat.forPattern( timestampFormat ).withZoneUTC();
        this.dataTypes = new HashSet<>( datamodel.ids() );
    }

    @Deprecated
    public void logWithTime( String filePreffix, Map<String, String> properties, String logType, String logSchemaId, int shard, String headers, String line ) {
        assert dataTypes.contains( logSchemaId );
        backend.log( Inet.HOSTNAME, filePreffix, properties, logType, logSchemaId, shard,
            timestampName + "\t" + headers, formatter.print( DateTimeUtils.currentTimeMillis() ) + "\t" + line );
    }

    public void log( String filePreffix, Map<String, String> properties, String logType, String logSchemaId, int shard, String headers, String line ) {
        assert dataTypes.contains( logSchemaId );
        backend.log( Inet.HOSTNAME, filePreffix, properties, logType, logSchemaId, shard, headers, line );
    }

    public boolean isLoggingAvailable() {
        return backend.isLoggingAvailable();
    }

    public AvailabilityReport availabilityReport() {
        return backend.availabilityReport();
    }
}
