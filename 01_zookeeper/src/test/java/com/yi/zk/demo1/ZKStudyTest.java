package com.yi.zk.demo1;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class ZKStudyTest {

    @Test
    public void createNode() throws Exception {
        // 重试机制
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectStr = "192.168.80.100:2181,192.168.80.110:2181,192.168.80.120:2181";

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 1000, 1000, retryPolicy);

        // 创建连接
        client.start();
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello2/world", "Hello World".getBytes());

        client.close();
    }
}
