package com.yi.demo1.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 自定义SplitBolt 将我们的单词统计出来
 *
 * @author huangwenyi
 * @date 2019-8-30
 */
public class CountBolt extends BaseBasicBolt {

    /**
     * 多线程保证安全
     */
    private static ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();

    /**
     * 上游来数据这个方法就会被调用
     *
     * @param input     上游发送的数据，都包在这个tuple里面了，我们可以从tuple当中获取上游发送的数据
     * @param collector 往下游发送数据的
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField("word");
//        Integer nums = input.getIntegerByField("nums");

        if (map.containsKey(word)){
//            map.put(word, map.get(word) + nums);
            map.put(word, map.get(word) + 1);
        }else {
//            map.put(word, nums);
            map.put(word, 1);
        }

        System.out.println("\033[32;4m" + "单词统计数量：" + map.toString() + "\033[0m");
    }

    /**
     * 数据处理完毕，没有下游 所以数据不需要往下传了
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
