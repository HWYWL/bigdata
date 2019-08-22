package com.yi.demo4.flow.sort.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 流量排序bean
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {
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
     * 对下行总流量倒叙排序
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowSortBean o) {
        return this.downCountFlow > o.downCountFlow ? -1 : 1;
    }

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

    public FlowSortBean() {

    }

    public FlowSortBean(Integer upFlow, Integer downFlow, Integer upCountFlow, Integer downCountFlow) {
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
