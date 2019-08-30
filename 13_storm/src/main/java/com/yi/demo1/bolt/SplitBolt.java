package com.yi.demo1.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 自定义SplitBolt 将我们的英文句子切割成一个个的单词
 *
 * @author huangwenyi
 * @date 2019-8-30
 */
public class SplitBolt extends BaseBasicBolt {
    /**
     * 处理输入元组并根据输入元组选择性地发出新的元组。
     * 这个方法也会反复不断的被调用，只要有上游发送的数据，这个方法就会执行
     * 所有的包装都是为您管理的。如果希望元组失败，则抛出FailedException。
     *
     * @param input     上游发送的数据，都包在这个tuple里面了，我们可以从tuple当中获取上游发送的数据
     * @param collector 往下游发送数据的
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.contains("hello")) {
            // 获取特定名称的字符串
            Object field = input.getValueByField("hello");

            if (null != field && !"".equals(field.toString())) {
                // 获取上游发送来的数据
                String line = field.toString();

                // 切割字符串继续往下传
                String[] split = line.split(" ");
                for (String word : split) {
                    collector.emit(new Values(word));
                }
            }
        }


    }

    /**
     * 声明此拓扑的所有流的输出模式。
     * 给所有往下传的数据一个声明，下游可以通过这个申请获取我们发送的数据
     *
     * @param declarer 用于声明输出流id、输出字段，以及是否每个输出流都是直接流
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("word", "nums"));
        declarer.declare(new Fields("word"));
    }
}
