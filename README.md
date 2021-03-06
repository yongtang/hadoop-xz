Hadoop-XZ [![Build Status](https://travis-ci.org/yongtang/hadoop-xz.svg?branch=master)](https://travis-ci.org/yongtang/hadoop-xz)
=========


XZ (LZMA/LZMA2) Codec for Apache Hadoop
--------------------------

Hadoop-XZ is a project to add the XZ compression codec in Hadoop.  XZ is a lossless data compression file format that incorporates the LZMA/LZMA2 compression algorithms.  XZ offers excellent compression ratio (LZMA/LZMA2) at the expense of longer compression time compared with other compression codecs such as gzip, lzo, or bzip2.  The decompression time of XZ is much more comparable with other compression codecs.  In fact, XZ have a much better decompression time than bzip2. It is an ideal compression format when longer compression time is tolerable.  The data can be divided into independently compressed blocks with the index of the blocks contained in the XZ file, which makes XZ a native splittable file format.

This library is built on top of the XZ Java library provided by http://tukaani.org (XZ Utils).  It supports the SplittableCompressionCodec interface so the individual XZ files could be processed with distributed tasks.  Keep in mind that XZ program tends to choose larger block size if no block size is specified (--block-size=size).  That often results in a single block within a huge compressed file.  This will not help distributed tasks.  It is always advised that an appropriate block size is specified when compression is performed.


Installation
------------
Add the hadoop-xz POM to a project with
```xml
<dependency>
  <groupId>io.sensesecure</groupId>
  <artifactId>hadoop-xz</artifactId>
  <version>1.4</version>
</dependency>
```
Or add project's SBT with 
```scala
libraryDependencies += "io.sensesecure" % "hadoop-xz" % "1.4"
```


Usage
-----
It is fairly simple to use XZ codec in Hadoop related programs.  For example, the following is an Apache Spark example of line count for an XZ compressed text file:

```scala
val sparkConf = new SparkConf().setAppName("Simple Application")
val sparkContext = new SparkContext(sparkConf)
val configuration = new Configuration()
configuration.set("io.compression.codecs","io.sensesecure.hadoop.xz.XZCodec")
val rdd = sparkContext.newAPIHadoopFile("sample.text.xz",
            classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
            configuration)

println(rdd.count())
```


Contact
-------
If you have trouble with the library or have questions, check out the GitHub repository at http://github.com/yongtang/hadoop-xz .
