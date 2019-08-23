package com.yi.demo1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * 数据重新组合
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class IndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count++;
        }

        context.write(key, new IntWritable(count));
    }
}
