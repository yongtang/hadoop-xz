package io.sensesecure.hadoop.xz;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

/**
 *
 * @author yongtang
 */
public class XZCodecTest {

    private final int count = 10000;
    private final int seed = new Random().nextInt();
    private final byte[] compressedTestData = {
        (byte) 0xfd, (byte) 0x37, (byte) 0x7a, (byte) 0x58,
        (byte) 0x5a, (byte) 0x00, (byte) 0x00, (byte) 0x04,
        (byte) 0xe6, (byte) 0xd6, (byte) 0xb4, (byte) 0x46,
        (byte) 0x02, (byte) 0x00, (byte) 0x21, (byte) 0x01,
        (byte) 0x1c, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x10, (byte) 0xcf, (byte) 0x58, (byte) 0xcc,
        (byte) 0x01, (byte) 0x00, (byte) 0x03, (byte) 0x74,
        (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x00,
        (byte) 0xa5, (byte) 0x75, (byte) 0x0c, (byte) 0xc1,
        (byte) 0xa7, (byte) 0xfd, (byte) 0x15, (byte) 0xfa,
        (byte) 0x00, (byte) 0x01, (byte) 0x1c, (byte) 0x04,
        (byte) 0x6f, (byte) 0x2c, (byte) 0x9c, (byte) 0xc1,
        (byte) 0x1f, (byte) 0xb6, (byte) 0xf3, (byte) 0x7d,
        (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x04, (byte) 0x59, (byte) 0x5a,
        (byte) 0x0a};
    private final byte[] uncompressedTestData = {'t', 'e', 's', 't'};

    private final int numberOfStreams = 3;
    private final long blocksize = 997;

    public XZCodecTest() {
    }

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @org.junit.Test
    public void testCreateOutputStream_OutputStream() throws Exception {
        System.out.println("createOutputStream");
        OutputStream out = new ByteArrayOutputStream();
        XZCodec instance = new XZCodec();
        Class<? extends CompressionOutputStream> expResult = XZCompressionOutputStream.class;
        Class<? extends CompressionOutputStream> result = instance.createOutputStream(out).getClass();
        assertEquals(expResult, result);
    }

    @org.junit.Test
    public void testCreateOutputStream_OutputStream_Compressor() throws Exception {
        System.out.println("createOutputStream");
        OutputStream out = new ByteArrayOutputStream();
        Compressor compressor = null;
        XZCodec instance = new XZCodec();
        Class<? extends CompressionOutputStream> expResult = XZCompressionOutputStream.class;
        Class<? extends CompressionOutputStream> result = instance.createOutputStream(out, compressor).getClass();
        assertEquals(expResult, result);
    }

    @org.junit.Test
    public void testGetCompressorType() {
        System.out.println("getCompressorType");
        XZCodec instance = new XZCodec();
        Class<? extends Compressor> expResult = XZCompressor.class;
        Class<? extends Compressor> result = instance.getCompressorType();
        assertEquals(expResult, result);
    }

    @org.junit.Test
    public void testCreateCompressor() {
        System.out.println("createCompressor");
        XZCodec instance = new XZCodec();
        Class<? extends Compressor> expResult = XZCompressor.class;
        Class<? extends Compressor> result = instance.createCompressor().getClass();
        assertEquals(expResult, result);
    }

    @org.junit.Test
    public void testCreateInputStream_InputStream() throws Exception {
        System.out.println("createInputStream");

        InputStream in = new ByteArrayInputStream(compressedTestData);
        XZCodec instance = new XZCodec();
        Class<? extends CompressionInputStream> expResult = XZCompressionInputStream.class;
        Class<? extends CompressionInputStream> result = instance.createInputStream(in).getClass();
        assertEquals(expResult, result);
    }

    @org.junit.Test
    public void testCreateInputStream_InputStream_Decompressor() throws Exception {
        System.out.println("createInputStream");
        InputStream in = new ByteArrayInputStream(compressedTestData);
        Decompressor decompressor = null;
        XZCodec instance = new XZCodec();
        Class<? extends CompressionInputStream> expResult = XZCompressionInputStream.class;
        Class<? extends CompressionInputStream> result = instance.createInputStream(in, decompressor).getClass();
        assertEquals(expResult, result);
    }

    @org.junit.Test
    public void testGetDecompressorType() {
        System.out.println("getDecompressorType");
        XZCodec instance = new XZCodec();
        Class<? extends Decompressor> expResult = XZDecompressor.class;
        Class<? extends Decompressor> result = instance.getDecompressorType();
        assertEquals(expResult, result);
    }

    @org.junit.Test
    public void testCreateDecompressor() {
        System.out.println("createDecompressor");
        XZCodec instance = new XZCodec();
        Class<? extends Decompressor> expResult = XZDecompressor.class;
        Class<? extends Decompressor> result = instance.createDecompressor().getClass();
        assertEquals(expResult, result);
    }

    @org.junit.Test
    public void testGetDefaultExtension() {
        System.out.println("getDefaultExtension");
        XZCodec instance = new XZCodec();
        String expResult = ".xz";
        String result = instance.getDefaultExtension();
        assertEquals(expResult, result);
    }

    @org.junit.Test
    public void testDecompression() throws IOException {
        System.out.println("Decompression");
        InputStream in = new ByteArrayInputStream(compressedTestData);
        XZCodec instance = new XZCodec();
        CompressionInputStream compressed = instance.createInputStream(in, null);
        byte[] uncompressed = new byte[uncompressedTestData.length + 1];
        assertEquals(compressed.read(uncompressed, 0, uncompressed.length), uncompressedTestData.length);
        assertArrayEquals(Arrays.copyOfRange(uncompressed, 0, uncompressedTestData.length), uncompressedTestData);
    }

    @org.junit.Test
    public void testCompressionDecompression() throws Exception {
        System.out.println("Compression/Decompression");

        XZCodec codec = new XZCodec();

        // Generate data
        DataOutputBuffer data = new DataOutputBuffer();
        RandomDatum.Generator generator = new RandomDatum.Generator(seed);
        for (int i = 0; i < count; ++i) {
            generator.next();
            RandomDatum key = generator.getKey();
            RandomDatum value = generator.getValue();
            key.write(data);
            value.write(data);
        }

        // Compress data
        DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
        CompressionOutputStream deflateFilter
                = codec.createOutputStream(compressedDataBuffer);
        DataOutputStream deflateOut
                = new DataOutputStream(new BufferedOutputStream(deflateFilter));
        deflateOut.write(data.getData(), 0, data.getLength());
        deflateOut.flush();
        deflateFilter.finish();

        // De-compress data
        DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
        deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
                compressedDataBuffer.getLength());
        CompressionInputStream inflateFilter
                = codec.createInputStream(deCompressedDataBuffer);
        DataInputStream inflateIn
                = new DataInputStream(new BufferedInputStream(inflateFilter));

        // Check
        DataInputBuffer originalData = new DataInputBuffer();
        originalData.reset(data.getData(), 0, data.getLength());
        DataInputStream originalIn = new DataInputStream(new BufferedInputStream(originalData));
        for (int i = 0; i < count; ++i) {
            RandomDatum k1 = new RandomDatum();
            RandomDatum v1 = new RandomDatum();
            k1.readFields(originalIn);
            v1.readFields(originalIn);
            RandomDatum k2 = new RandomDatum();
            RandomDatum v2 = new RandomDatum();
            k2.readFields(inflateIn);
            v2.readFields(inflateIn);
            assertTrue("original and compressed-then-decompressed-output not equal",
                    k1.equals(k2) && v1.equals(v2));

            // original and compressed-then-decompressed-output have the same hashCode
            Map<RandomDatum, String> m = new HashMap<>();
            m.put(k1, k1.toString());
            m.put(v1, v1.toString());
            String result = m.get(k2);
            assertEquals("k1 and k2 hashcode not equal", result, k1.toString());
            result = m.get(v2);
            assertEquals("v1 and v2 hashcode not equal", result, v1.toString());
        }

        // De-compress data byte-at-a-time
        originalData.reset(data.getData(), 0, data.getLength());
        deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
                compressedDataBuffer.getLength());
        inflateFilter
                = codec.createInputStream(deCompressedDataBuffer);

        // Check
        originalIn = new DataInputStream(new BufferedInputStream(originalData));
        int expected;
        do {
            expected = originalIn.read();
            assertEquals("Inflated stream read by byte does not match",
                    expected, inflateFilter.read());
        } while (expected != -1);
    }

    @org.junit.Test
    public void testSequenceFileCompressionDecompression() throws Exception {
        System.out.println("Sequence File Compression/Decompression");
        XZCodec codec = new XZCodec();

        // Generate data and sequence file
        final DataOutputBuffer data = new DataOutputBuffer();
        final Configuration conf = new Configuration();
        final File file = new File(folder.getRoot(), "test.seq");
        final Path path = new Path("file:" + file.getAbsolutePath());
        try (Writer writer = SequenceFile.createWriter(conf,
                Writer.file(path),
                Writer.keyClass(RandomDatum.class),
                Writer.valueClass(RandomDatum.class),
                Writer.compression(CompressionType.BLOCK, codec))) {
            RandomDatum.Generator generator = new RandomDatum.Generator(seed);
            int numSyncs = 4;
            for (int i = 0, syncEvery = count / (numSyncs + 1), nextSync = syncEvery; i < count; ++i) {
                generator.next();
                RandomDatum key = generator.getKey();
                RandomDatum value = generator.getValue();
                key.write(data);
                value.write(data);
                writer.append(key, value);
                if (i >= nextSync) {
                    // Ensure that the codec copes with multiple blocks per file
                    writer.sync();
                    nextSync += syncEvery;
                }
            }
        }

        // Read the sequence file and check its contents
        DataInputBuffer originalData = new DataInputBuffer();
        originalData.reset(data.getData(), 0, data.getLength());
        DataInputStream originalIn = new DataInputStream(new BufferedInputStream(originalData));
        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(path))) {
            for (int i = 0; i < count; ++i) {
                RandomDatum k1 = new RandomDatum();
                RandomDatum v1 = new RandomDatum();
                k1.readFields(originalIn);
                v1.readFields(originalIn);
                RandomDatum k2 = new RandomDatum();
                RandomDatum v2 = new RandomDatum();
                reader.next(k2, v2);
                assertTrue("original and compressed-then-decompressed-output not equal",
                        k1.equals(k2) && v1.equals(v2));

                // original and compressed-then-decompressed-output have the same hashCode
                Map<RandomDatum, String> m = new HashMap<>();
                m.put(k1, k1.toString());
                m.put(v1, v1.toString());
                String result = m.get(k2);
                assertEquals("k1 and k2 hashcode not equal", result, k1.toString());
                result = m.get(v2);
                assertEquals("v1 and v2 hashcode not equal", result, v1.toString());
            }
        }
    }

    @org.junit.Test
    public void testSplittableCompressionDecompression() throws Exception {
        System.out.println("SplittableCompression/Decompression");

        Configuration conf = new Configuration();
        conf.setLong("xz.presetlevel", 5);
        conf.setLong("xz.blocksize", blocksize);
        XZCodec codec = new XZCodec(conf);

        Random random = new Random(seed);

        byte[] datum = new byte[count];
        for (int i = 0; i < count; i++) {
            datum[i] = (byte) (random.nextInt() & 0xFF);
        }
        byte[] data = new byte[count * numberOfStreams];
        byte[] checkData = new byte[count * numberOfStreams];

        for (int streamIndex = 0; streamIndex < numberOfStreams; streamIndex++) {
            System.arraycopy(datum, 0, data, count * streamIndex, count);
        }
        for (int streamIndex = 0; streamIndex < numberOfStreams; streamIndex++) {
            for (int i = 0; i < count; i++) {
                assertEquals(datum[i], data[count * streamIndex + i]);
            }
        }

        DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
        for (int streamIndex = 0; streamIndex < numberOfStreams; streamIndex++) {

            // Compress data
            CompressionOutputStream deflateFilter
                    = codec.createOutputStream(compressedDataBuffer);
            DataOutputStream deflateOut
                    = new DataOutputStream(new BufferedOutputStream(deflateFilter));
            deflateOut.write(datum, 0, datum.length);
            deflateOut.flush();
            deflateFilter.finish();
        }

        java.nio.file.Path tmp = Files.createTempFile("hadoop-xz-", "-tmp");

        Files.write(tmp, Arrays.copyOf(compressedDataBuffer.getData(), compressedDataBuffer.size()));
        tmp.toFile().deleteOnExit();

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream inData = fileSystem.open(new Path(tmp.toAbsolutePath().toString()));

        final int nextStep = 997;
        long start = 0;
        int offset = 0;
        while (start < compressedDataBuffer.size()) {
            long end = start + nextStep < compressedDataBuffer.size() ? start + nextStep : compressedDataBuffer.size();

            // De-compress data
            SplitCompressionInputStream inflateFilter = codec.createInputStream(inData, null, start, end, READ_MODE.BYBLOCK);
            DataInputStream inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));

            int chunk = inflateIn.read(checkData, offset, checkData.length - offset);
            chunk = chunk == -1 ? 0 : chunk;
            offset += chunk;

            start = end;
        }
        assertEquals(offset, data.length);
        assertArrayEquals(data, checkData);

        // De-compress data
        DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
        deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, compressedDataBuffer.getLength());
        CompressionInputStream inflateFilter = codec.createInputStream(deCompressedDataBuffer);
        DataInputStream inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));

        // Check
        inflateIn.readFully(checkData, 0, checkData.length);
        assertArrayEquals(data, checkData);

    }

}
