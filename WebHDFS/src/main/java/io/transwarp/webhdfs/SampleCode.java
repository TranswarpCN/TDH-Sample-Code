package io.transwarp.webhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by XKJ on 2016/11/23.
 */

public class SampleCode {
    public static void main(String[] args) throws IOException, URISyntaxException, AuthenticationException {
//        testPseudo();
//        URI uri = URI.create(WebHdfsFileSystem.SCHEME+"://172.16.2.97:50070");
//
//        Configuration conf = new Configuration();
//        conf.addResource("conf/hdfs-site.xml");
//        conf.addResource("conf/core-site.xml");
////        conf.set("hadoop.security.authentication", "kerberos");
//
////        conf.set("hadoop.http.staticuser.user","hdfs");
////        System.out.println(conf.get("hadoop.http.staticuser.user"));
//        UserGroupInformation.setConfiguration(conf);
//        UserGroupInformation.loginUserFromPassword("hdfs", "123456");
////        UserGroupInformation.loginUserFromKeytab("hdfs@TDH","./conf/hdfs.keytab");
//        System.out.println(WebHdfsFileSystem.SCHEME);
//
//        WebHdfsFileSystem fs = (WebHdfsFileSystem)FileSystem.get(uri,conf);
//        Path path = new Path("/");
////        fs.create(path);
//        FileStatus[] files = fs.listStatus(path);
//        for(FileStatus ft:files){
//            System.out.println(ft.getOwner());
//        }

        WebHDFSConnectionFactory webHDFSConnectionFactory = new WebHDFSConnectionFactory("172.16.2.96",50070, "hdfs",
                "123456", WebHDFSConnectionFactory.AuthenticationType.KERBEROS.name());
        WebHDFSConnection connection = webHDFSConnectionFactory.getConnection();

        String json = "";

        try {
            //getHomeDirectory
//            json = connection.getHomeDirectory();
//            System.out.println(json);
            // open
//            OutputStream os =new FileOutputStream("E:\\workplace\\core-site.xml");
//            json = connection.open("tmp/hdfs-site.xml",os);
//            System.out.println(json);

            //getContentSummary
//            json = connection.getContentSummary("tmp/hdfs-site.xml");
//            System.out.println(json);

            //listStatus
//            json = connection.listStatus("tmp/hdfs-site.xml");
//            System.out.println(json);

            //getFileStatus
//            json = connection.getFileStatus("tmp/hdfs-site.xml");
//            System.out.println(json);

            //getFileCheckSum
//            json = connection.getFileCheckSum("tmp/hdfs-site.xml");
//            System.out.println(json);
            //create
//            InputStream is =new FileInputStream("E:\\workplace\\core-site.xml");
//            json = connection.create("tmp/hdfs-site00.xml",is);
//            System.out.println(json);

            //mkdirs
//            json = connection.mkdirs("tmp/一指禅");
//            System.out.println(json);

            //createSymLink TODO
//            json = connection.createSymLink("tmp/hdfs-site.xml","/tmp/hdfs-site00.xml");
//            System.out.println(json);

            //rename
//            json = connection.rename("tmp/hdfs-site.xml","/tmp/hdfs-site0g0.xml");
//            System.out.println(json);

            //setPermission
//            json = connection.setPermission("tmp/hdfs-site0g0.xml");
//            System.out.println(json);

            //setOwner
//            json = connection.setOwner("tmp/hdfs-site0g0.xml");
//            System.out.println(json);

            //setReplication
//            json = connection.setReplication("tmp/hdfs-site0g0.xml");
//            System.out.println(json);

            //setTimes
//            json = connection.setTimes("tmp/hdfs-site0g0.xml");
//            System.out.println(json);

            //append
//            InputStream is =new FileInputStream("E:\\workplace\\core-site.xml");
//            json = connection.append("tmp/hdfs-site0g0.xml",is);
//            System.out.println(json);

            //delete
            json = connection.delete("tmp/hdfs-site0g0.xml");
            System.out.println(json);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (AuthenticationException e) {
            e.printStackTrace();
        }
    }

    public static void testPseudo(){

        WebHDFSConnectionFactory webHDFSConnectionFactory = new WebHDFSConnectionFactory("172.16.1.35",50070, "hdfs",
                "123456", WebHDFSConnectionFactory.AuthenticationType.PSEUDO.name());
        WebHDFSConnection connection = webHDFSConnectionFactory.getConnection();

        try {
            String json = "";
            //getHomeDirectory
            //System.out.println( connection.getHomeDirectory());

            //open
//            OutputStream os = new FileOutputStream(new File("E:\\workplace\\core-site.xml"));
//            String json = connection.open("user/hdfs/core-site.xml",os);
//            System.out.println(json);

            //getContentSummary
//            json = connection.getContentSummary("user/hdfs/core-site.xml");
//            System.out.println(json);

            //listStatus
//            json = connection.listStatus("user/hdfs/core-site.xml");
//            System.out.println(json);

            //getFileStatus
//            json = connection.getFileStatus("user/hdfs/core-site.xml");
//            System.out.println(json);

            //getFileCheckSum
//            json = connection.getFileCheckSum("user/hdfs/core-site.xml");
//            System.out.println(json);

            //create
//            InputStream is = new FileInputStream(new File("E:\\workplace\\core-site.xml"));
//            json = connection.create("user/hdfs/core-site.xml00",is);
//            System.out.println(json);

            //mkdirs
//            json = connection.mkdirs("user/hdfs/webhdfs");
//            System.out.println(json);

            //createSymLink TODO
//            json = connection.createSymLink("user/hdfs/core-site.xml006","/user/hdfs/core-site.xml00");
//            System.out.println(json);

            //rename
//            json = connection.rename("user/hdfs/core-site.xml00","/user/hdfs/core-site.xml006");
//            System.out.println(json);
            //setPermission
//            json = connection.setPermission("user/hdfs/core-site.xml006");
//            System.out.println(json);

            //setOwner
//            json = connection.setOwner("user/hdfs/core-site.xml006");
//            System.out.println(json);

            //setReplication
//            json = connection.setReplication("user/hdfs/core-site.xml006");
//            System.out.println(json);

            //setTimes
//            json = connection.setTimes("user/hdfs/core-site.xml006");
//            System.out.println(json);

            //append
//            InputStream is = new FileInputStream(new File("E:\\workplace\\core-site.xml"));
//            json = connection.append("user/hdfs/core-site.xml006",is);
//            System.out.println(json);

            ///user/hdfs/core-site.xml delete
            json = connection.delete("user/hdfs/core-site.xml006");
            System.out.println(json);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (AuthenticationException e) {
            e.printStackTrace();
        }

    }
}
