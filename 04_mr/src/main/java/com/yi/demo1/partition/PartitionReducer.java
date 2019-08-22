package com.yi.demo1.partition;

import org.apache.hadoop.io.NullWritable;
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
public class PartitionReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 不做任何事直接把数据往下传，把v3设置为空 我们这里不需要这个数据
        context.write(key, NullWritable.get());
    }
}
