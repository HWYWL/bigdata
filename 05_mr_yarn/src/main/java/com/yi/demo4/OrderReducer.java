package com.yi.demo4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 订单价格拼接
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class OrderReducer extends Reducer<OrderBean, Text, Text, Text> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value).append("\t");
        }

        context.write(new Text(key.getOrderId()), new Text(builder.toString()));
    }
}
