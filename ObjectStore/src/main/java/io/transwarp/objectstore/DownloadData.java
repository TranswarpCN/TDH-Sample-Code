package io.transwarp.objectstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import static io.transwarp.objectstore.MD5Util.md5crypt;

public class DownloadData {
    private Connector connector;
    private Configuration configuration;
    private Constant constant;
    private MD5Util md5Util;

    /**
     * 构造函数
     */
    public DownloadData() {
        connector = new Connector();
        constant = new Constant();
        configuration = connector.getConfiguration();
    }

    /**
     * 获取文件
     * @param fileName 文件名
     */
    public int getFile(String fileName) {
        try {
            HTable hTable = new HTable(configuration, constant.HBASE_TABLE_NAME);
            String fileName2 = fileName;
            if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                fileName2 = constant.UPLOAD_DIR + "\\" + fileName;
                System.out.println(fileName2);
            } else {
                fileName2 = constant.UPLOAD_DIR + "/" + fileName;
            }
            String fileNameMD5 = md5crypt(fileName2);

            String result_filename;
            byte[] result_filedata;
            int result_fileCount=0;

            byte [] startRowKey = Bytes.toBytes(fileNameMD5);
            byte [] stopRowKey = Bytes.toBytes(fileNameMD5+"9");

            Scan scan = new Scan();
            scan.setStartRow(startRowKey);
            scan.setStopRow(stopRowKey);
            ResultScanner scanner = hTable.getScanner(scan);
            for (Result r : scanner) {
                result_filename = Bytes.toString(r.getValue(Bytes.toBytes("file"), Bytes.toBytes("filename")));
                System.out.println(result_filename);
                if (fileName2.compareTo(result_filename) == 0){
                    result_fileCount++;
                    result_filedata = r.getValue(Bytes.toBytes("data"), Bytes.toBytes("bytes"));
                    FileUtil.byte2File(result_filedata, constant.DOWNLOAD_DIR, fileName);
                }
            }
            if (result_fileCount != 0)
                System.out.println("Download "+ result_fileCount + " files");
            else
                System.out.println("Not found:"+fileName);

            hTable.close();
            return result_fileCount;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    // 关闭连接
    public void disConnect() {
        connector.close();
    }

    // 主函数
    public static void main(String[] args) {
        // 下载时，更改文件名
        String fileName = "JR-0076(1).ai";
        DownloadData downloadData = new DownloadData();
        int k = downloadData.getFile(fileName);
        downloadData.disConnect();
        if (k == 0) {
            if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                fileName = downloadData.constant.UPLOAD_DIR + "\\" + fileName;
            } else {
                fileName = downloadData.constant.UPLOAD_DIR + "/" + fileName;
            }
            String[] h = md5crypt(fileName).split("/");
            String hdfsFile = downloadData.constant.HDFS_LARGE_FILE_DIR + "/" + h[h.length-1];
            Downloader downloader = new Downloader(hdfsFile);
            downloader.download(downloadData.constant.DOWNLOAD_DIR);
        }
    }
}
