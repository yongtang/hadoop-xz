package io.sensesecure.hadoop.xz;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

/**
 *
 * @author yongtang
 */
public class XZSplittableCodec extends XZCodec implements SplittableCompressionCodec {

    public XZSplittableCodec(Configuration conf) {
        super(conf);
    }

    @Override
    public SplitCompressionInputStream createInputStream(InputStream seekableIn, Decompressor decompressor, long start, long end, READ_MODE readMode) throws IOException {
        return new XZSplitCompressionInputStream(seekableIn, start, end, readMode);
    }

}
