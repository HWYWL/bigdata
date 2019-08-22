package com.yi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * 上传文件到HDFS文件系统
     */
    @Test
    public void uploadFile() throws URISyntaxException, IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url), new Configuration());

        fileSystem.copyFromLocalFile(new Path("D:\\gitCode\\bigdata\\04_mr\\src\\main\\resources\\flow\\data_flow.dat"),
                new Path("/test/flow/data_flow.dat"));

        fileSystem.close();
    }

    /**
     * 删除HDFS文件
     */
    @Test
    public void deleteFile() throws URISyntaxException, IOException {
        String url = "hdfs://node01.hadoop.com:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url), new Configuration());

        fileSystem.delete(new Path("/test/outpartition"), true);
        fileSystem.close();
    }
}
