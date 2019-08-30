package com.yi.demo1.sport;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * 接受外部数据源的组件，将外部数据源转化成Storm内部的数据，以Tuple为基本的传输单元下发给Bolt
 *
 * @author huangwenyi
 * @date 2019-8-30
 */
public class RandomSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    Random rand;
    String[] sentences;

    /**
     * 系统初始化，比如连接kafka，读取数据，连接MySQL，或者连接Redis的初始化操作
     *
     * @param map                  系统初始化读取配置文件
     * @param topologyContext      应用上下文对象
     * @param spoutOutputCollector 用于接收spout输出的数据
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        rand = new Random();
//        sentences = new String[]{"hello world", "hello storm", "hadoop hive", "hello kitty", "sqoop hadoop"};
    }

    /**
     * 在storm框架中，会一直调用nestTuple将数据不断的往后发送，发送给下一个组件当中
     */
    @Override
    public void nextTuple() {
        String sentence = sentences[rand.nextInt(sentences.length)];
        try {
            Thread.sleep(1000);
            // 把数据发送到下游
            collector.emit(new Values(sentence));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 为我们往下游发送的单词申请一个字符串
     * 下游获取单词的时候，可以通过这个字符串获取
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hello"));
    }
}
