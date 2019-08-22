package com.yi.demo2.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<PairSort, Text, PairSort, Text> {
    @Override
    protected void reduce(PairSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 自定义规约
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
