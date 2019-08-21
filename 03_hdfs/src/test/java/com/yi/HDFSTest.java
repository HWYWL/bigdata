package com.yi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Unit test for simple App.
 */
public class HDFSTest {
    /**
     * 获取FileSystem方式1
     */
    @Test
    public void getFileSystem1() throws URISyntaxException, IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        Configuration entries = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI(url), entries);

        System.out.println(fileSystem.toString());
        fileSystem.close();
    }

    /**
     * 获取FileSystem方式2
     */
    @Test
    public void getFileSystem2() throws URISyntaxException, IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS", url);

        FileSystem fileSystem = FileSystem.get(new URI("/"), entries);

        System.out.println(fileSystem.toString());
        fileSystem.close();
    }

    /**
     * 获取FileSystem方式3
     */
    @Test
    public void getFileSystem3() throws URISyntaxException, IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        Configuration entries = new Configuration();

        FileSystem fileSystem = FileSystem.newInstance(new URI(url), entries);

        System.out.println(fileSystem.toString());
        fileSystem.close();
    }

    /**
     * 获取FileSystem方式4
     */
    @Test
    public void getFileSystem4() throws IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS", url);

        FileSystem fileSystem = FileSystem.newInstance(entries);

        System.out.println(fileSystem.toString());
        fileSystem.close();
    }

    /**
     * 通过递归获取文件系统的所有文件
     */
    @Test
    public void listFile() throws IOException, URISyntaxException {
        String url = "hdfs://node01.hadoop.com:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url), new Configuration());

        // 获取根目录下的所有文件
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().toString());
        }

        fileSystem.close();
    }

    /**
     * 下载HDFS文件到本地
     */
    @Test
    public void getFileToLocal() throws URISyntaxException, IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url), new Configuration());

        fileSystem.copyToLocalFile(new Path("/test/input/hadoop-root-datanode-node01.hadoop.com.log"), new Path("D:\\Temp\\hadoop.log"));

        fileSystem.close();
    }

    /**
     * 在HDFS文件系统中创建文件夹
     */
    @Test
    public void mkdirs() throws URISyntaxException, IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url), new Configuration());

        fileSystem.mkdirs(new Path("/test/output/"));

        fileSystem.close();
    }

    /**
     * 上传文件到HDFS文件系统
     */
    @Test
    public void uploadFile() throws URISyntaxException, IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url), new Configuration());

        fileSystem.copyFromLocalFile(new Path("D:\\Temp\\hadoop.log"), new Path("/test/output/"));

        fileSystem.close();
    }

    /**
     * 合并小文件为大文件上传到HDFS文件系统
     */
    @Test
    public void mergeFile() throws URISyntaxException, IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url), new Configuration());
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test/input/merge.xml"), true);

        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        FileStatus[] listStatus = local.listStatus(new Path("D:\\gitCode\\bigdata\\03_hdfs\\src\\test\\resources"));

        for (FileStatus status : listStatus) {
            FSDataInputStream inputStream = local.open(status.getPath());

            IOUtils.copyBytes(inputStream, outputStream, 1024);
        }

        fileSystem.close();
        local.close();
    }
}
