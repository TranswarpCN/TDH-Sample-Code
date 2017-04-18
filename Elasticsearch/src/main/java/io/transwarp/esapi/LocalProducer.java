package io.transwarp.esapi;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 杨发林 on 2016/11/22.
 */
public class LocalProducer implements  Producer{

    /*
    log
     */
    private static final Log log = LogFactory.getLog(LocalProducer.class);
    /*
    local file path
     */
    private static final String BULK_IMPORT_DATA_DIR_KEY = "bulk.import.data.local.dir";
    /*
    local  file directory
     */
    private final File directory;
    /*
    local files list
     */
    private final List<File> fileList;
    /*
   Compression Code
     */
    private final CompressionCodec codec;
    /*

     */
    private int current = 0;
    /*
    Constructed function;
     */
    public LocalProducer(CompressionCodec codec) {
        this.codec = codec;

        String dir = System.getProperty(BULK_IMPORT_DATA_DIR_KEY);
        if (dir == null) {
            throw new RuntimeException("missing config " + BULK_IMPORT_DATA_DIR_KEY);
        }
        directory = new File(dir);
        if (!directory.exists()) {
            throw new RuntimeException("dir " + dir + " is not exists");
        }
        if (!directory.isDirectory()) {
            throw new RuntimeException("dir " + dir + " is not directory");
        }
        fileList = new ArrayList<File>();
        findAllFiles(directory);
    }
    /*
    find all files in the directory
     */
    private void findAllFiles(File dir) {
        File[] files = dir.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                findAllFiles(f);
            } else {
                log.info("discover a new file: " + f.getAbsolutePath() + "/" + f.getName());
                fileList.add(f);
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
                    File file = fileList.get(current++);
                    InputStream is = createInputStreamFromFile(file);
                    log.info(is.toString() + " - " + file.getAbsolutePath()+file.getName());
                    return is;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /*
    get the file inputStream
     */
    protected InputStream createInputStreamFromFile(File file) throws IOException {
        if (null != this.codec) {
            return codec.createInputStream(new FileInputStream(file), this.codec.createDecompressor());
        } else {
            return new FileInputStream(file);
        }
    }
}
