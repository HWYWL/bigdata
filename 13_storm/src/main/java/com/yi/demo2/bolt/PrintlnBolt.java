package com.yi.demo2.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class PrintlnBolt extends BaseBasicBolt {
    /**
     * 接收上游数据
     * @param input 上游数据
     * @param collector
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Object value = input.getValue(4);
        System.out.println(input.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
