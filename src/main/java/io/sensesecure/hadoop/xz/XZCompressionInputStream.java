package io.sensesecure.hadoop.xz;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.tukaani.xz.XZInputStream;

/**
 *
 * @author yongtang
 */
public class XZCompressionInputStream extends CompressionInputStream {

    private final BufferedInputStream bufferedIn;

    private final XZInputStream xzIn;

    public XZCompressionInputStream(InputStream in) throws IOException {
        super(in);
        bufferedIn = new BufferedInputStream(super.in);
        xzIn = new XZInputStream(bufferedIn);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return xzIn.read(b, off, len);
    }

    @Override
    public void resetState() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int read() throws IOException {
        return xzIn.read();
    }

    @Override
    public void close() throws IOException {
        xzIn.close();
    }

    @Override
    public long getPos() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
