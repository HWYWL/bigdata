package com.yi.demo5.flow.partition;

import com.yi.demo5.flow.partition.entity.PhonePartitionBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 数据拆分
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class PhonePartitionMapper extends Mapper<LongWritable, Text, Text, PhonePartitionBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String phoneNum = split[1];
        String upFlow = split[6];
        String downFlow = split[7];
        String upCountFlow = split[8];
        String downCountFlow = split[9];

        PhonePartitionBean flowBean = new PhonePartitionBean(
                Integer.parseInt(upFlow),
                Integer.parseInt(downFlow),
                Integer.parseInt(upCountFlow),
                Integer.parseInt(downCountFlow)
        );

        // 把每个手机号码的流量处理之后发送到reduce
        context.write(new Text(phoneNum), flowBean);
    }
}
