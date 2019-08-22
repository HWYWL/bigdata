package com.yi.demo6.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 数据拆分
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取文件切片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        // 获取文件名
        String name = inputSplit.getPath().getName();

        String[] split = value.toString().split(",");
        // 商品文件信息
        if ("orders.txt".equals(name)){
            context.write(new Text(split[2]), value);
        }else {
            // 订单文件信息
            context.write(new Text(split[0]), value);
        }
    }
}
