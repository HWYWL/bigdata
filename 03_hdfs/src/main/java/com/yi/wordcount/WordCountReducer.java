package com.yi.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer 数据聚合
 * k2  v2   k3  v3  四个泛型
 *
 * @author huangwenyi
 * @date 2019-8-21
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable intWritable  =  new IntWritable();

    /**
     * @param key     k2
     * @param values  v2集合
     * @param context 上下文切换
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        intWritable.set(count);

        // 将数据写出
        context.write(key, intWritable);
    }
}
