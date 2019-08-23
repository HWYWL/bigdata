package com.yi.demo2;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 实现文件一次性读取
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeFileRecordReader recordReader = new WholeFileRecordReader();

        // 初始化自定义文件读取参数
        recordReader.initialize(split, context);

        return recordReader;
    }
}
