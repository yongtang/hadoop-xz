package io.sensesecure.hadoop.xz;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 *
 * @author yongtang
 */
public class XZCodec implements CompressionCodec {

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return new XZCompressionOutputStream(out);
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
