package com.yi.demo5.flow.partition.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 流量分区bean
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class PhonePartitionBean implements Writable {
    /**
     * 上行流量
     */
    private Integer upFlow;
    /**
     * 下行流量
     */
    private Integer downFlow;
    /**
     * 上行总流量
     */
    private Integer upCountFlow;
    /**
     * 下行总流量
     */
    private Integer downCountFlow;

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(upCountFlow);
        out.writeInt(downCountFlow);
    }

    /**
     * 反序列化
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.upCountFlow = in.readInt();
        this.downCountFlow = in.readInt();
    }

    public PhonePartitionBean() {

    }

    public PhonePartitionBean(Integer upFlow, Integer downFlow, Integer upCountFlow, Integer downCountFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.upCountFlow = upCountFlow;
        this.downCountFlow = downCountFlow;
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    @Override
    public String toString() {
        return "FlowSortBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", upCountFlow=" + upCountFlow +
                ", downCountFlow=" + downCountFlow +
                '}';
    }
}
