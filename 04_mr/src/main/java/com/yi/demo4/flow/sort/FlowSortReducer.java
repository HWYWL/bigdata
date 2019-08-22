package com.yi.demo4.flow.sort;

import com.yi.demo4.flow.sort.entity.FlowSortBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 每个手机号码使用诗句统计
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class FlowSortReducer extends Reducer<FlowSortBean, Text, FlowSortBean, Text> {
    @Override
    protected void reduce(FlowSortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
