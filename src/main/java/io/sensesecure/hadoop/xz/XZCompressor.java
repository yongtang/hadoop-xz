package io.sensesecure.hadoop.xz;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 *
 * @author yongtang
 */
public class XZCompressor implements Compressor {

    @Override
    public void setInput(byte[] b, int off, int len) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean needsInput() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getBytesRead() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getBytesWritten() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void finish() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean finished() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int compress(byte[] b, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void reset() {
        // do nothing
    }

    @Override
    public void end() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void reinit(Configuration conf) {
        // do nothing
    }
}
