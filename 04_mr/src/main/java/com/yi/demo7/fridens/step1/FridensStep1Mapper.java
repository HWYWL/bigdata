package com.yi.demo7.fridens.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 数据拆分
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class FridensStep1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");

        String[] fridens = split[1].split(",");

        for (String friden : fridens) {
            context.write(new Text(friden), new Text(split[0]));
        }
    }
}
