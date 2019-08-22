package com.yi.demo5.flow.partition;

import com.yi.demo5.flow.partition.entity.PhonePartitionBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区逻辑
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class PhonePartition extends Partitioner<Text, PhonePartitionBean> {
    @Override
    public int getPartition(Text text, PhonePartitionBean phonePartitionBean, int numPartitions) {
        String subStr = text.toString().substring(0, 3);
        int partitionsIndex = 0;
        switch (subStr) {
            case "135":
                partitionsIndex = 0;
                break;
            case "136":
                partitionsIndex = 1;
                break;
            case "137":
                partitionsIndex = 2;
                break;
            case "138":
                partitionsIndex = 3;
                break;
            case "139":
                partitionsIndex = 4;
                break;
            default:
                partitionsIndex = 5;
                break;
        }

        return partitionsIndex;
    }
}
