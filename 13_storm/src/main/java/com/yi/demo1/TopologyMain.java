package com.yi.demo1;

import com.yi.demo1.bolt.CountBolt;
import com.yi.demo1.bolt.SplitBolt;
import com.yi.demo1.sport.RandomSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 统计单词出现的次数
 *
 * @author huangwenyi
 * @date 2019-8-30 16:24:37
 */
public class TopologyMain {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // 将我们的spout与bolt组织成一个topology
        // 通过TopologyBuilder组织我们的spout与bolt
        TopologyBuilder builder = new TopologyBuilder();

        // 设置我们的spout,设置三个线程来执行我们的spout的nextTuple方法
        builder.setSpout("randomSpout", new RandomSpout(), 3);

        // 设置分组策略 也就是定义上游数据是谁
        builder.setBolt("splitBolt", new SplitBolt(), 3).localOrShuffleGrouping("randomSpout");
        builder.setBolt("countBolt", new CountBolt(), 3).localOrShuffleGrouping("splitBolt");

        // 提交代码
        Config config = new Config();
        // 设置进程数
        config.setNumWorkers(3);

        // 集群提交模式：打包到集群上面去的时候使用这种方式提交
        StormTopology topology = builder.createTopology();
        if (args != null && args.length > 0) {
            // 带有参数
            config.setDebug(false);
            StormSubmitter.submitTopology(args[0], config, topology);
        } else {
            config.setDebug(true);
            // 本地提交模式，便于我们开发调试
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCount", config, topology);
        }
    }
}
