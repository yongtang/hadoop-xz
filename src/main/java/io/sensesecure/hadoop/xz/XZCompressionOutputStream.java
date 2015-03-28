package io.sensesecure.hadoop.xz;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

/**
 *
 * @author yongtang
 */
public class XZCompressionOutputStream extends CompressionOutputStream {

    private final BufferedOutputStream bufferedOut;

    private final XZOutputStream xzOut;

    public XZCompressionOutputStream(OutputStream out) throws IOException {
        super(out);
        bufferedOut = new BufferedOutputStream(out);
        xzOut = new XZOutputStream(bufferedOut, new LZMA2Options(6));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        xzOut.write(b, off, len);
    }

    @Override
    public void finish() throws IOException {
        xzOut.finish();
    }

    @Override
    public void resetState() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void write(int b) throws IOException {
        xzOut.write(b);
    }

    @Override
    public void flush() throws IOException {
        xzOut.flush();
    }

    @Override
    public void close() throws IOException {
        xzOut.flush();
        xzOut.close();
    }
}
