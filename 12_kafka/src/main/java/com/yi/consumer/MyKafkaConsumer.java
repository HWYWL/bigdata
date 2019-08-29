package com.yi.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 消费者
 *
 * @author huangwenyi
 * @date 2019-8-28
 */
public class MyKafkaConsumer {
    public static void main(String[] args) {
        /**
         * 自动提交offset
         *
         */
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.put("group.id", "test");
        //关闭我们的offset的自动提交
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //   insertIntoDb(buffer);
                //手动提交offset的值
                consumer.commitSync();
                buffer.clear();
            }
        }
    }
}
