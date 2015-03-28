package io.sensesecure.hadoop.xz;

import java.io.IOException;
import org.apache.hadoop.io.compress.Decompressor;

/**
 *
 * @author yongtang
 */
public class XZDecompressor implements Decompressor {

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
    public boolean needsDictionary() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean finished() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int decompress(byte[] b, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getRemaining() {
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

}
