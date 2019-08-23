package com.yi.demo1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 数据拆分
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class IndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取文件切片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        // 获取文件名
        String name = inputSplit.getPath().getName();

        String[] split = value.toString().split(" ");

        for (String text : split) {
            // k2文本加上文件名 k3 为1
            context.write(new Text(text + "-" + name), new IntWritable(1));
        }

    }
}
