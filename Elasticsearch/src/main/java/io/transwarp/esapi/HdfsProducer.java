package io.transwarp.esapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 杨发林 on 2016/11/22.
 */
public class HdfsProducer  implements  Producer{

    /*
    hdfs file path
     */
    private static final String BULK_IMPORT_DATA_DIR_KEY = "bulk.import.data.hdfs.dir";
    /*
    Compression Code
     */
    private final CompressionCodec codec;
    /*
    Decompressor
     */
    private final Decompressor decompressor;
    /*
    hadoop Configuration object
     */
    private final Configuration conf;
    /*
    hdfs FileSystem
     */
    private final FileSystem fs;
    /*
    hdfs file list
     */
    private final List<Path> fileList;
    /*
    file num
     */
    private int current = 0;

    /*

     */
    public HdfsProducer(CompressionCodec codec, Configuration conf) throws IOException {
        this.codec = codec;
        if (this.codec != null) {
            decompressor = this.codec.createDecompressor();
        } else {
            decompressor = null;
        }
        this.conf = conf;

        String dir = System.getProperty(BULK_IMPORT_DATA_DIR_KEY);
        if (dir == null) {
            throw new RuntimeException("missing config " + BULK_IMPORT_DATA_DIR_KEY);
        }

        Path path = new Path(dir);
        fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
            throw new RuntimeException("dir " + dir + " is not exists");
        }
        if (!fs.isDirectory(path)) {
            throw new RuntimeException("dir " + dir + " is not directory");
        }
        fileList = new ArrayList<Path>();
        findAllFiles(path);
    }

    /*
    find all files in hdfs file system
     */
    private void findAllFiles(Path dir) throws IOException {
        FileStatus[] status = fs.listStatus(dir);
        for (FileStatus s : status) {
            if (s.isDirectory()) {
                findAllFiles(s.getPath());
            } else {
                fileList.add(s.getPath());
            }
        }
    }

    /*
    get the producer inputStream
     */
    public InputStream next() {
        while (true) {
            if (current >= fileList.size()) {
                return null;
            } else {
                try {
                    Path path = fileList.get(current++);
                    return createInputStreamFromFile(path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /*
    get the file inputStream
     */
    protected InputStream createInputStreamFromFile(Path path) throws IOException {
        if (decompressor != null) {
            return codec.createInputStream(fs.open(path), decompressor);
        } else {
            return fs.open(path);
        }
    }
}
