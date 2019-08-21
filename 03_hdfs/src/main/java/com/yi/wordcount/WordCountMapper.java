package com.yi.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text text = new Text();
    private IntWritable intWritable = new IntWritable();

    /**
     * 数据逻辑处理
     *
     * @param key     key1   行偏移量 ，一般没啥用，直接可以丢掉
     * @param value   value1   行文本内容，需要切割，然后转换成新的k2  v2  输出
     * @param context 上下文切换
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        for (String word : split) {
            text.set(word);
            intWritable.set(1);

            // 写出k2  v2  这里的类型跟我们的k2  v2  保持一致
            context.write(text, intWritable);
        }
    }
}
