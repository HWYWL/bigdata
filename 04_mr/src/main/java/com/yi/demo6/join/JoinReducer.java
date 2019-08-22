package com.yi.demo6.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 数据重新组合
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();
        for (Text value : values) {
            String data = value.toString();
            if (data.startsWith("p")) {
                buffer.insert(0, data).insert(data.length() - 1, "\t");
            } else {
                buffer.append(data);
            }
        }

        context.write(key, new Text(buffer.toString()));
    }
}
