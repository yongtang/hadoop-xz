package io.sensesecure.hadoop.xz;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;
import org.tukaani.xz.SeekableXZInputStream;
import org.tukaani.xz.XZ;
import org.tukaani.xz.check.Check;
import org.tukaani.xz.common.DecoderUtil;
import org.tukaani.xz.common.StreamFlags;
import static org.tukaani.xz.common.Util.BLOCK_HEADER_SIZE_MAX;
import static org.tukaani.xz.common.Util.STREAM_HEADER_SIZE;

/**
 *
 * @author yongtang
 */
public class XZSplitCompressionInputStream extends SplitCompressionInputStream {

    private final READ_MODE readMode;

    private final XZSeekableInputStream xzSeekableIn;

    private final SeekableXZInputStream seekableXZIn;

    private long adjustedStart;

    private long adjustedEnd;

    private long uncompressedStart;

    private long uncompressedEnd;

    public XZSplitCompressionInputStream(InputStream seekableIn, long start, long end, READ_MODE readMode) throws IOException {
        super(seekableIn, start, end);

        this.readMode = readMode;

        if (!(seekableIn instanceof Seekable)) {
            throw new IOException("seekableIn must be an instance of " + Seekable.class.getName());
        }

        // There are two ways to find the stream footer:
        // 1. If each block includes the compressedSize, then the
        // stream footer could be located by skip through the
        // blocks until the index indicator is encountered. Since
        // compressedSize is an optional field in the block, this
        // method is not guaranteed.
        // 2. Alternatively the stream footer could be located by
        // scan though the stream until the footer magic is found
        DataInputStream inData = new DataInputStream(seekableIn);

        byte[] streamHeaderBuf = new byte[STREAM_HEADER_SIZE];
        byte[] streamFooterBuf = new byte[STREAM_HEADER_SIZE];
        byte[] buf = new byte[BLOCK_HEADER_SIZE_MAX];

        long streamFinal = 0, offsetFinal = 0;

        ((Seekable) seekableIn).seek(0);
        int streamHeaderLen = inData.read(streamHeaderBuf, 0, STREAM_HEADER_SIZE);
        while (streamHeaderLen != -1) {
            if (streamHeaderLen != STREAM_HEADER_SIZE) {
                throw new IOException("XZ Stream Header is corrupt");
            }
            StreamFlags streamFlags = DecoderUtil.decodeStreamHeader(streamHeaderBuf);
            int checkSize = Check.getInstance(streamFlags.checkType).getSize();

            // Block Header or Index Indicator
            inData.readFully(buf, 0, 1);
            if (streamFinal != 0 && buf[0] != 0x00) {
                // We get the offsetFinal and stop
                offsetFinal = ((Seekable) seekableIn).getPos() - 1;
                break;
            }
            while (buf[0] != 0x00) {
                // Read the rest of the block header
                int headerSize = 4 * ((buf[0] & 0xFF) + 1);
                inData.readFully(buf, 1, headerSize - 1);

                // Validate the CRC32
                if (!DecoderUtil.isCRC32Valid(buf, 0, headerSize - 4, headerSize - 4)) {
                    throw new IOException("XZ Block Header is corrupt");
                }

                // Check for reserved bits in Block Flags
                if ((buf[1] & 0x3C) != 0) {
                    throw new IOException("Unsupported options in XZ Block Header");
                }

                // Check compressed size
                if ((buf[1] & 0x40) == 0x00) {
                    // No compressed size
                    break;
                }
                ByteArrayInputStream bufStream = new ByteArrayInputStream(buf, 2, headerSize - 6);
                long compressedSize = DecoderUtil.decodeVLI(bufStream);
                if (compressedSize == 0 || compressedSize > ((DecoderUtil.VLI_MAX & ~(4 - 1)) - headerSize - checkSize)) {
                    throw new IOException("Corrupted compressed size in XZ Block Header");
                }
                // Skip compressed, padding, and crc32
                long remaining = ((compressedSize + checkSize) + 4 - 1) & ~(4 - 1);
                if (inData.skip(remaining) != remaining) {
                    throw new IOException("Incomplete data in XZ Block");
                }

                inData.readFully(buf, 0, 1);
            }

            // No compressed size
            if (buf[0] != 0x00) {
                // Locate footer after end
                ((Seekable) seekableIn).seek((end & ~(4 - 1)) < STREAM_HEADER_SIZE ? 0 : ((end & ~(4 - 1)) - STREAM_HEADER_SIZE));
                while (true) {
                    inData.readFully(streamFooterBuf, 8, 4);
                    if (streamFooterBuf[10] == XZ.FOOTER_MAGIC[0] && streamFooterBuf[11] == XZ.FOOTER_MAGIC[1]) {
                        ((Seekable) seekableIn).seek(((Seekable) seekableIn).getPos() - STREAM_HEADER_SIZE);
                        inData.readFully(streamFooterBuf);
                        if (streamFooterBuf[10] == XZ.FOOTER_MAGIC[0] && streamFooterBuf[11] == XZ.FOOTER_MAGIC[1] && DecoderUtil.isCRC32Valid(streamFooterBuf, 4, 6, 0)) {
                            break;
                        }
                    }
                }

                if (((Seekable) seekableIn).getPos() >= end) {
                    offsetFinal = streamFinal = ((Seekable) seekableIn).getPos();
                }

                // Stream header
                streamHeaderLen = inData.read(streamHeaderBuf, 0, STREAM_HEADER_SIZE);
                continue;
            }

            // Index Indicator
            long count = DecoderUtil.decodeVLI(inData);

            // If the Record count doesn't fit into an int, we cannot allocate the array to hold the record
            if (count > Integer.MAX_VALUE) {
                throw new IOException("XZ Index has over " + Integer.MAX_VALUE + " Records");
            }
            // Decode the record
            for (int i = (int) count; i > 0; --i) {
                // Get the next Record
                long unpaddedSize = DecoderUtil.decodeVLI(inData);
                long uncompressedSize = DecoderUtil.decodeVLI(inData);
            }

            // Padding + CRC32
            long off = ((Seekable) seekableIn).getPos();
            long padding = ((off + 4 - 1) & ~(4 - 1)) - off;
            inData.skip(padding + 4);

            // Stream Footer
            inData.readFully(streamFooterBuf);
            if (streamFooterBuf[10] == XZ.FOOTER_MAGIC[0] && streamFooterBuf[11] == XZ.FOOTER_MAGIC[1] && DecoderUtil.isCRC32Valid(streamFooterBuf, 4, 6, 0)) {
                throw new IOException("XZ Stream Footer is corrupt");
            }

            if (((Seekable) seekableIn).getPos() >= end) {
                offsetFinal = streamFinal = ((Seekable) seekableIn).getPos();
            }

            // Stream Header
            streamHeaderLen = inData.read(streamHeaderBuf, 0, STREAM_HEADER_SIZE);
        }

        xzSeekableIn = new XZSeekableInputStream(in, streamFinal);

        seekableXZIn = new SeekableXZInputStream(xzSeekableIn);

        if (seekableXZIn.getBlockCount() == 0) {
            adjustedStart = 0;
            uncompressedStart = 0;
            if (start != 0) {
                adjustedStart = offsetFinal;
                uncompressedStart = seekableXZIn.length();
            }
            adjustedEnd = 0;
            uncompressedEnd = 0;
            if (end != 0) {
                adjustedEnd = offsetFinal;
                uncompressedEnd = seekableXZIn.length();
            }
        } else {
            adjustedStart = 0;
            uncompressedStart = 0;
            if (start != 0) {
                for (int i = 1; i < seekableXZIn.getBlockCount(); i++) {
                    if (start <= seekableXZIn.getBlockCompPos(i)) {
                        adjustedStart = seekableXZIn.getBlockCompPos(i);
                        uncompressedStart = seekableXZIn.getBlockPos(i);
                        break;
                    }
                }
                if (adjustedStart == 0) {
                    adjustedStart = offsetFinal;
                    uncompressedStart = seekableXZIn.length();
                }
            }
            adjustedEnd = 0;
            uncompressedEnd = 0;
            if (end != 0) {
                for (int i = 1; i < seekableXZIn.getBlockCount(); i++) {
                    if (end <= seekableXZIn.getBlockCompPos(i)) {
                        adjustedEnd = seekableXZIn.getBlockCompPos(i);
                        uncompressedEnd = seekableXZIn.getBlockPos(i);
                        break;
                    }
                }
                if (adjustedEnd == 0) {
                    adjustedEnd = offsetFinal;
                    uncompressedEnd = seekableXZIn.length();
                }
            }
        }
        seekableXZIn.seek(this.uncompressedStart);
    }

    @Override
    public void close() throws IOException {
        seekableXZIn.close();
    }

    @Override
    public long getAdjustedStart() {
        return adjustedStart;
    }

    @Override
    public long getAdjustedEnd() {
        return adjustedEnd;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (seekableXZIn.position() < uncompressedEnd) {
            len = (int) ((seekableXZIn.position() + len < uncompressedEnd) ? len : uncompressedEnd - seekableXZIn.position());
            return seekableXZIn.read(b, off, len);
        }
        return -1;
    }

    @Override
    public void resetState() throws IOException {
        throw new UnsupportedOperationException("readState() is not supported yet.");
    }

    @Override
    public int read() throws IOException {
        byte b[] = new byte[1];
        int result = this.read(b, 0, 1);
        return (result < 0) ? result : (b[0] & 0xff);
    }

    @Override
    public long getPos() throws IOException {
        if (seekableXZIn.position() < uncompressedEnd) {
            return adjustedStart;
        }
        return adjustedEnd;
    }
}
