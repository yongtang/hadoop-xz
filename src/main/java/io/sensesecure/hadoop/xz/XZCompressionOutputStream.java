package io.sensesecure.hadoop.xz;

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

    private XZOutputStream xzOut;
    
    private boolean resetStateNeeded;

    public XZCompressionOutputStream(OutputStream out) throws IOException {
        super(out);
        resetStateNeeded = true;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(6));
        }
        xzOut.write(b, off, len);
    }

    @Override
    public void finish() throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(6));
        }
        xzOut.finish();
        resetStateNeeded = true;
    }

    @Override
    public void resetState() throws IOException {
        resetStateNeeded = true;
    }

    @Override
    public void write(int b) throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(6));
        }
        xzOut.write(b);
    }

    @Override
    public void flush() throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(6));
        }
        xzOut.flush();
    }

    @Override
    public void close() throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(6));
        }
        xzOut.flush();
        xzOut.close();
        resetStateNeeded = false;
    }
}
