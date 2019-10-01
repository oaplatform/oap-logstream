package oap.logstream.tsv;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by igor.petrenko on 2019-10-01.
 */
public class TsvInputStream extends FastBufferedInputStream {
    public final IntArrayList cells = new IntArrayList();

    public TsvInputStream(InputStream is) {
        super(is);
    }

    public boolean readCells(byte[] line) throws IOException {
        var len = readLine(line);

        if (len <= 0) return false;

        Tsv.split(line, len, cells);

        return true;
    }
}
