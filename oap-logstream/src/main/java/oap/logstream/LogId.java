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

import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.net.Inet;
import oap.util.Strings;
import org.joda.time.DateTime;

import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


@ToString
@EqualsAndHashCode( exclude = "clientHostname" )
public class LogId implements Serializable {
    @Serial
    private static final long serialVersionUID = -6026646143366760882L;
    public final String logType;
    public final String clientHostname;
    public final int shard;
    public final String headers;

    public final String filePrefixPattern;
    public final LinkedHashMap<String, String> properties = new LinkedHashMap<>();

    public LogId( String filePrefixPattern, String logType, String clientHostname, int shard, Map<String, String> properties, String headers ) {
        this.filePrefixPattern = filePrefixPattern;
        this.logType = logType;
        this.clientHostname = clientHostname;
        this.shard = shard;
        this.properties.putAll( properties );
        this.headers = headers;
    }

    public final String fileName( String fileSuffixPattern, DateTime time, Timestamp timestamp, int version ) {
        var suffix = fileSuffixPattern;
        if( fileSuffixPattern.startsWith( "/" ) && filePrefixPattern.endsWith( "/" ) ) suffix = suffix.substring( 1 );
        else if( !fileSuffixPattern.startsWith( "/" ) && !filePrefixPattern.endsWith( "/" ) ) suffix = "/" + suffix;

        var pattern = filePrefixPattern + suffix;
        if( pattern.startsWith( "/" ) ) pattern = pattern.substring( 1 );

        return Strings.substitute( pattern, v -> switch( v ) {
            case "LOG_TYPE" -> logType;
            case "LOG_VERSION" -> version;
            case "SERVER_HOST" -> Inet.HOSTNAME;
            case "CLIENT_HOST" -> clientHostname;
            case "SHARD" -> shard;
            case "YEAR" -> time.getYear();
            case "MONTH" -> print2Chars( time.getMonthOfYear() );
            case "DAY" -> print2Chars( time.getDayOfMonth() );
            case "HOUR" -> print2Chars( time.getHourOfDay() );
            case "INTERVAL" -> print2Chars( timestamp.currentBucket( time ) );
            case "REGION" -> System.getenv( "REGION" );
            default -> {
                var res = properties.get( v );
                if( res == null )
                    throw new IllegalArgumentException( "Unknown variable '" + v + "'" );

                yield res;
            }
        } );
    }

    private String print2Chars( int v ) {
        return v > 9 ? String.valueOf( v ) : "0" + v;
    }

    public final String lock() {
        return ( String.join( "-", properties.values() )
            + String.join( "-", List.of( filePrefixPattern, logType, String.valueOf( shard ), headers ) ) ).intern();
    }
}
