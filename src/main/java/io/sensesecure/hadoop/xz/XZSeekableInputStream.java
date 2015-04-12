package io.sensesecure.hadoop.xz;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.Seekable;
import org.tukaani.xz.SeekableInputStream;

/**
 *
 * @author yongtang
 */
public class XZSeekableInputStream extends SeekableInputStream {

    private final InputStream seekableIn;
    private final long length;

    XZSeekableInputStream(InputStream in, long len) {
        seekableIn = in;
        length = len;
    }

    @Override
    public long length() throws IOException {
        return length;
    }

    @Override
    public long position() throws IOException {
        return ((Seekable) seekableIn).getPos();
    }

    @Override
    public void seek(long pos) throws IOException {
        ((Seekable) seekableIn).seek(pos);
    }

    @Override
    public int read() throws IOException {
        if (((Seekable) seekableIn).getPos() <length) {
            return seekableIn.read();
        }
        return -1;
    }

}
