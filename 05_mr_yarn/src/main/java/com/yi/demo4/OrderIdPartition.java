package com.yi.demo4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 对订单id进行分区
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class OrderIdPartition extends Partitioner<OrderBean, Text> {
    /**
     * @param orderBean     k2
     * @param text  v2
     * @param numPartitions reduce的个数
     * @return
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int numPartitions) {
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
