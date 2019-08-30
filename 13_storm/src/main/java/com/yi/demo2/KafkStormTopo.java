package com.yi.demo2;

import com.yi.demo2.bolt.PrintlnBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 集成kafka
 *
 * @author huangwenyi
 * @date 2019-8-30
 */
public class KafkStormTopo {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // 通过内部类获取kafka配置
        KafkaSpoutConfig.Builder<String, String> kafkaConfigBuilder = KafkaSpoutConfig.builder("node01:9092,node02:9092,node03:9092", "test");

        // 控制kafka的offset的消费策略
        kafkaConfigBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);
        kafkaConfigBuilder.setGroupId("kafkaStorm");
        kafkaConfigBuilder.setOffsetCommitPeriodMs(1000L);

        KafkaSpoutConfig<String, String> kafkaSpoutConfig = kafkaConfigBuilder.build();
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("printlnBolt", new PrintlnBolt()).localOrShuffleGrouping("kafkaSpout");

        Config config = new Config();
        if (null != args && args.length > 0) {
            // 集群模式提交
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            // 本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("localStorm", config, builder.createTopology());
        }
    }
}
