package com.yi.demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义文件读取
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit split;
    private Configuration configuration;
    private BytesWritable bytesWritable = new BytesWritable();
    /**
     * 读取文件完毕设置为true
     */
    private boolean  processed  = false;

    /**
     * 初始化时调用一次。
     *
     * @param split   定义要读取的记录范围的分割
     * @param context 任务的信息
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    /**
     * 读取下一个键值对。
     *
     * @return 如果读取了键/值对，则为true
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed){
            Path path = split.getPath();

            FileSystem fileSystem = FileSystem.get(configuration);
            // 读取文件流
            FSDataInputStream stream = fileSystem.open(path);

            // 把流一次性读取到字节数组
            byte[] bytes = new byte[(int) split.getLength()];

            IOUtils.readFully(stream, bytes, 0, bytes.length);
            bytesWritable.set(bytes, 0, bytes.length);

            processed = true;

            IOUtils.closeStream(stream);
            fileSystem.close();

            return true;
        }

        return false;
    }

    /**
     * 获取当前键
     *
     * @return k1 如果没有当前键，则为null
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获取当前值。
     *
     * @return v1 被读取的对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    /**
     * 记录读取器通过其数据的当前进程。
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    /**
     * 关闭记录读取器。
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }
}
