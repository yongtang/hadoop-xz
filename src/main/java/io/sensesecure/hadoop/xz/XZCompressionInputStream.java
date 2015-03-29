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

    private BufferedInputStream bufferedIn;

    private XZInputStream xzIn;

    private boolean resetStateNeeded;

    public XZCompressionInputStream(InputStream in) throws IOException {
        super(in);
        resetStateNeeded = false;
        bufferedIn = new BufferedInputStream(super.in);
        xzIn = new XZInputStream(bufferedIn);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            bufferedIn = new BufferedInputStream(super.in);
            xzIn = new XZInputStream(bufferedIn);
        }
        return xzIn.read(b, off, len);
    }

    @Override
    public void resetState() throws IOException {
        resetStateNeeded = true;
    }

    @Override
    public int read() throws IOException {
        byte b[] = new byte[1];
        int result = this.read(b, 0, 1);
        return (result < 0) ? result : (b[0] & 0xff);
    }

    @Override
    public void close() throws IOException {
        if (!resetStateNeeded) {
            xzIn.close();
            resetStateNeeded = true;
        }
    }
}
