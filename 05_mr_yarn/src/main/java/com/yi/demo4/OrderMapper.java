package com.yi.demo4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 数据拆分
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        OrderBean order = new OrderBean(split[0], Double.parseDouble(split[2]));

        context.write(order, NullWritable.get());
    }
}
