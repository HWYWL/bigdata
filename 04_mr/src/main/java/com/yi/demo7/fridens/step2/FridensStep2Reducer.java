package com.yi.demo7.fridens.step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 数据重新组合
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class FridensStep2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();

        // 根据分类之后的好友拼接起来
        for (Text value : values) {
            buffer.append(value.toString()).append("\t");
        }

        context.write(key, new Text(buffer.toString()));
    }
}
