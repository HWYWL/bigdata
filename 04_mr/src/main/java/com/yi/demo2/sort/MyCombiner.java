package com.yi.demo2.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 注意自定义combiner的话，这里的输入类型以及输出类型，都是key2  value2
 * 可以减少输出到reduce的key2的个数
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class MyCombiner extends Reducer<PairSort, Text, PairSort, Text> {
    @Override
    protected void reduce(PairSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 自定义规约
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
