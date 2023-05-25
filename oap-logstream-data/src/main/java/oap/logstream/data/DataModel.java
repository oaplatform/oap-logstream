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

package oap.logstream.data;

import lombok.extern.slf4j.Slf4j;
import oap.dictionary.Dictionary;
import oap.dictionary.DictionaryParser;
import oap.dictionary.DictionaryRoot;
import oap.io.content.Resource;

import javax.annotation.Nonnull;

import static oap.dictionary.DictionaryParser.INCREMENTAL_ID_STRATEGY;

@Slf4j
public class DataModel {
    public final Dictionary model;

    public DataModel( @Nonnull DictionaryRoot model ) {
        this.model = model;
    }

    public DataModel( @Nonnull Resource resource ) {
        log.debug( "loading {}", resource.url );
        this.model = DictionaryParser.parse( resource.url, INCREMENTAL_ID_STRATEGY );
    }
}
