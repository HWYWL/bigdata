package com.yi.demo5.flow.partition;

import com.yi.demo5.flow.partition.entity.PhonePartitionBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * 分区文件合并
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class PhonePartitionReducer extends Reducer<Text, PhonePartitionBean, Text, PhonePartitionBean> {
    @Override
    protected void reduce(Text key, Iterable<PhonePartitionBean> values, Context context) throws IOException, InterruptedException {
        int  upFlow = 0;
        int downFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;

        for (PhonePartitionBean value : values) {
            upFlow += value.getUpFlow();
            downFlow  += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }

        //写出去我们的手机号
        PhonePartitionBean flowBean = new PhonePartitionBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setDownCountFlow(downCountFlow);

        context.write(key,flowBean);
    }
}
