package io.transwarp.objectstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileUtil {
    /**
     * file转byte
     * @param file 文件名
     * @return byte文件
     * @throws Exception 文件为空
     */
    public static byte[] file2Byte(File file) throws Exception {
        FileInputStream fileInputStream = new FileInputStream(file);
        byte[] data = new byte[fileInputStream.available()];
        fileInputStream.read(data);
        fileInputStream.close();
        return data;
    }

    /**
     * byte转file
     * @param bytes byte文件
     * @param path 文件路径
     * @param fileName 文件名
     * @return 文件
     */
    public static File byte2File(byte[] bytes, String path, String fileName) {
        if (bytes == null)
            return null;

        File file = new File(path + "\\" + fileName);
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(file);
            fileOutputStream.write(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileOutputStream != null) {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return file;
    }
}