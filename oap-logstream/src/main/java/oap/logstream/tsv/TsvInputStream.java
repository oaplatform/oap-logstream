package oap.logstream.tsv;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by igor.petrenko on 2019-10-01.
 */
public class TsvInputStream extends FastBufferedInputStream {
    public final Line line;

    public TsvInputStream( InputStream is, byte[] bytes ) {
        super( is );

        line = new Line( bytes );
    }

    public boolean readCells() throws IOException {
        line.cells.clear();
        var buffer = line.buffer;
        var len = readLine( buffer );

        line.len = len;

        if( len <= 0 ) return false;

        Tsv.split( buffer, len, line.cells );

        return true;
    }

    @ToString
    public static class Line {
        public final byte[] buffer;
        public final IntArrayList cells = new IntArrayList();
        public int len = 0;

        public Line( byte[] buffer ) {
            this.buffer = buffer;
        }

        public int indexOf( String value ) {
            for( int i = 0; i < cells.size(); i++ ) {
                var offset = i == 0 ? 0 : cells.getInt( i - 1 );
                var length = cells.getInt( i ) - offset - 1;
                var str = new String( buffer, offset, length, UTF_8 );

                if( value.equals( str ) ) return i;
            }

            return -1;
        }
    }
}
