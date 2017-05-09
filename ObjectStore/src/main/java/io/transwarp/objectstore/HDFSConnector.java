package io.transwarp.objectstore;

import org.apache.hadoop.conf.Configuration;

public class HDFSConnector {
    public static Configuration getHDFSConf() {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("yarn-site.xml");
        return conf;
    }
}
