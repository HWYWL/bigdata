package com.yi.demo7.fridens.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 数据重新组合
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class FridensStep1Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder buffer = new StringBuilder();
        for (Text value : values) {
            buffer.append(value.toString()).append("-");
        }

        //往外写出去数据  k3 A-E-   v3 B
        context.write(new Text(buffer.toString()), key);
    }
}
