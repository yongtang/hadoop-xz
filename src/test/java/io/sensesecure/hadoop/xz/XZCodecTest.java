package io.sensesecure.hadoop.xz;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

/**
 *
 * @author yongtang
 */
public class XZCodecTest {

    public XZCodecTest() {
    }

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
        OutputStream out = null;
        XZCodec instance = new XZCodec();
        CompressionOutputStream expResult = null;
        CompressionOutputStream result = instance.createOutputStream(out);
        //assertEquals(expResult, result);
        //fail("The test case is a prototype.");
    }

    @org.junit.Test
    public void testCreateOutputStream_OutputStream_Compressor() throws Exception {
        System.out.println("createOutputStream");
        OutputStream out = null;
        Compressor compressor = null;
        XZCodec instance = new XZCodec();
        CompressionOutputStream expResult = null;
        CompressionOutputStream result = instance.createOutputStream(out, compressor);
        //assertEquals(expResult, result);
        //fail("The test case is a prototype.");
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
        InputStream in = null;
        XZCodec instance = new XZCodec();
        //CompressionInputStream expResult = null;
        //CompressionInputStream result = instance.createInputStream(in);
        //assertEquals(expResult, result);
        //fail("The test case is a prototype.");
    }

    @org.junit.Test
    public void testCreateInputStream_InputStream_Decompressor() throws Exception {
        System.out.println("createInputStream");
        InputStream in = null;
        Decompressor decompressor = null;
        XZCodec instance = new XZCodec();
        //CompressionInputStream expResult = null;
        //CompressionInputStream result = instance.createInputStream(in, decompressor);
        //assertEquals(expResult, result);
        //fail("The test case is a prototype.");
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

}
