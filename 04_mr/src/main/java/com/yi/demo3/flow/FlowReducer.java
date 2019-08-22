package com.yi.demo3.flow;

import com.yi.demo3.flow.entity.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 每个手机号码使用诗句统计
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int downFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;

        for (FlowBean bean : values) {
            upFlow += bean.getUpFlow();
            downFlow += bean.getDownFlow();
            upCountFlow += bean.getUpCountFlow();
            downCountFlow += bean.getDownCountFlow();
        }

        FlowBean flowBean = new FlowBean(upFlow, downFlow, upCountFlow, downCountFlow);
        context.write(key, flowBean);
    }
}
