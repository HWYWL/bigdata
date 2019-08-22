package com.yi.demo2.sort;

import com.yi.demo2.sort.entity.Sort;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 组装一个新的k2 v2
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class SortMapper extends Mapper<LongWritable, Text, PairSort, Text> {
    private PairSort pairSort = new PairSort();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 通过context获取自定义计数器
        Counter counter = context.getCounter("MAP_COUNTER", "MAP_INPUT_RECORDS");
        counter.increment(1L);

        String[] split = value.toString().split("\t");
        pairSort.getSort().setFirst(split[0]);
        pairSort.getSort().setSecond(Integer.parseInt(split[1]));

        context.write(pairSort, value);
    }
}
