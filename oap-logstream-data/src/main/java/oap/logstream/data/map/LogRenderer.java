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

package oap.logstream.data.map;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.reflect.Reflect;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static oap.util.Strings.UNKNOWN;

@ToString
@EqualsAndHashCode
public class LogRenderer {
    public final String headers;
    private final List<String> expressions;

    public LogRenderer( String headers, List<String> expressions ) {
        this.headers = headers;
        this.expressions = expressions;
    }

    public String render( Map<String, Object> value ) {
        StringJoiner joiner = new StringJoiner( "\t" );
        for( String expression : expressions ) {
            Object v = Reflect.get( value, expression );
            joiner.add( v instanceof Boolean
                ? ( Boolean ) v ? "1" : "0"
                : v == null ? ""
                    : Objects.equals( UNKNOWN, v ) ? "" : String.valueOf( v ) );
        }
        return joiner.toString();
    }
}
