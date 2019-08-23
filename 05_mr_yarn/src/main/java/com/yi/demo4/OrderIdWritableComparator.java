package com.yi.demo4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class OrderIdWritableComparator extends WritableComparator {
    public OrderIdWritableComparator() {
        super(OrderBean.class, true);

    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean)a;
        OrderBean second = (OrderBean)b;

        //比较两个orderId是否相同，如果相同，就会把相同的orderId的数据弄到一个集合里面去
        return first.getOrderId().compareTo(second.getOrderId());

    }
}
