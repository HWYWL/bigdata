package com.yi.demo1.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map 数据拆分
 * k1  v1   k2  v2  四个泛型
 *
 * @author huangwenyi
 * @date 2019-8-21
 */
public class PartitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    /**
     * 不做事直接把数据往下传
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把v2设置为空 我们这里不需要这个数据
        context.write(value, NullWritable.get());
    }
}
