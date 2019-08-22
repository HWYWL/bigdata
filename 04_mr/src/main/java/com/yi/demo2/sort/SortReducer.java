package com.yi.demo2.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 合并结果集
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class SortReducer extends Reducer<PairSort, Text, PairSort, NullWritable> {
    @Override
    protected void reduce(PairSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
