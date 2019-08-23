package com.yi.demo3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输出
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException {
        Path outputPath = getOutputPath(job);
        Configuration configuration = job.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);

        //好评的输出流
        FSDataOutputStream goodComment = fileSystem.create(new Path(outputPath + "\\good_comment\\good_comment.txt"));
        //差评的输出流
        FSDataOutputStream badComment = fileSystem.create(new Path(outputPath + "\\bad_comment\\bad_comment.txt"));

        return new MyRecorderWriter(goodComment, badComment);
    }
}
