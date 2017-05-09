package io.transwarp.objectstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class CreateDir {
    public static void main(String[] args) throws IOException {
        // 通过Java API创建HDFS目录
        Constant constant = new Constant();
        String rootPath = "hdfs://nameservice1";
        System.out.println(rootPath + constant.HDFS_LARGE_FILE_DIR);
        Path p = new Path(rootPath + constant.HDFS_LARGE_FILE_DIR);

        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("yarn-site.xml");
        // 没开kerberos，注释下面两行
        FileSystem fs = p.getFileSystem(conf);
        boolean b = fs.mkdirs(p);
        System.out.println(b);
        fs.close();
    }
}