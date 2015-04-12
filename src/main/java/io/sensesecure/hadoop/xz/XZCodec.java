package io.sensesecure.hadoop.xz;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

/**
 *
 * @author yongtang
 */
public class XZCodec implements Configurable, SplittableCompressionCodec {

    private static final int PRESET_LEVEL_DEFAULT = 6;
    private static final long BLOCK_SIZE_DEFAULT = Long.MAX_VALUE;

    private Configuration conf;

    public XZCodec() {
        this.conf = new Configuration();
    }

    public XZCodec(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public SplitCompressionInputStream createInputStream(InputStream seekableIn, Decompressor decompressor, long start, long end, READ_MODE readMode) throws IOException {
        return new XZSplitCompressionInputStream(seekableIn, start, end, readMode);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return new XZCompressionOutputStream(out, conf.getInt("xz.presetlevel", PRESET_LEVEL_DEFAULT), conf.getLong("xz.blocksize", BLOCK_SIZE_DEFAULT));
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
        return createOutputStream(out);
    }

    @Override
    public Class<? extends Compressor> getCompressorType() {
        return XZCompressor.class;
    }

    @Override
    public Compressor createCompressor() {
        return new XZCompressor();
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in) throws IOException {
        return new XZCompressionInputStream(in);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
        return createInputStream(in);
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        return XZDecompressor.class;
    }

    @Override
    public Decompressor createDecompressor() {
        return new XZDecompressor();
    }

    @Override
    public String getDefaultExtension() {
        return ".xz";
    }
}
