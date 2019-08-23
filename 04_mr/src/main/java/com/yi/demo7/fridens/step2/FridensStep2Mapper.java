package com.yi.demo7.fridens.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * 数据拆分
 * 用户                 共同好友
 * F-D-O-I-H-B-K-G-C-	A
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class FridensStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把共同好友和用户风格开
        String[] split = value.toString().split("\t");
        //按照  -  切割成为一个数组
        String[] user = split[0].split("-");

        // 排序 防止重复 例如 F-D D-F
        Arrays.sort(user);

        for (int i = 0; i < user.length - 1; i++) {
            for (int j = i + 1; j < user.length; j++) {
                // 拼接两个用户之间的共同好友
                context.write(new Text(user[i] + "-" + user[j]), new Text(split[1]));
            }
        }
    }
}
