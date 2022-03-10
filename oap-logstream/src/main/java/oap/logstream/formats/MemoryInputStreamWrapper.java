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

package oap.logstream.formats;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;

public class MemoryInputStreamWrapper extends FastByteArrayInputStream implements Seekable, PositionedReadable {
    public MemoryInputStreamWrapper( InputStream is, int size ) throws IOException {
        super( new byte[size] );

        IOUtils.readFully( is, array );
    }

    @Override
    public int read( long position, byte[] buffer, int offset, int length ) throws IOException {
        int availableLength = Math.min( length, length - ( int ) position );

        System.arraycopy( array, ( int ) position, buffer, offset, availableLength );

        return availableLength;
    }

    @Override
    public void readFully( long position, byte[] buffer, int offset, int length ) throws IOException {
        System.arraycopy( array, ( int ) position, buffer, offset, length );
    }

    @Override
    public void readFully( long position, byte[] buffer ) throws IOException {
        System.arraycopy( array, ( int ) position, buffer, 0, buffer.length );
    }

    @Override
    public void seek( long pos ) throws IOException {
        position( pos );
    }

    @Override
    public long getPos() throws IOException {
        return position();
    }

    @Override
    public boolean seekToNewSource( long targetPos ) throws IOException {
        return false;
    }
}
