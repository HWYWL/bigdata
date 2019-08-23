package com.yi.demo3;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 自定义输出
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class MyRecorderWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream goodOutputStream;
    private FSDataOutputStream badOutputStream;
    /**
     * 通过构造器传参
     * @param goodOutputStream 好评输出
     * @param badOutputStream 差评输出
     * @throws IOException
     */
    public MyRecorderWriter(FSDataOutputStream goodOutputStream, FSDataOutputStream badOutputStream) throws IOException {
        this.goodOutputStream = goodOutputStream;
        this.badOutputStream = badOutputStream;
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 获取一行数据
        String[] split = key.toString().split("\t");
        // 获取评论状态 0：好评，1：中评，2：差评
        String commentStatus = split[9];

        if (Integer.parseInt(commentStatus) <= 1) {
            // 把数据写入文件
            goodOutputStream.write(key.toString().getBytes());
            goodOutputStream.write("\r\n".getBytes());
        } else {
            // 把数据写入文件
            badOutputStream.write(key.toString().getBytes());
            badOutputStream.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(goodOutputStream);
        IOUtils.closeStream(badOutputStream);
    }
}
