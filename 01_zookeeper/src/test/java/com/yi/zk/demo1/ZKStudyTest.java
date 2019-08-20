package com.yi.zk.demo1;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class ZKStudyTest {

    /**
     * 创建节点
     * @throws Exception
     */
    @Test
    public void createNode() throws Exception {
        // 重试机制
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectStr = "node01.hadoop.com:2181,node02.hadoop.com:2181,node03.hadoop.com:2181";

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 1000, 1000, retryPolicy);

        // 创建连接
        client.start();

        // 创建永久节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello/world1", "Hello World1".getBytes());
        // 创建永久有序节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/hello/world2", "Hello World2".getBytes());
        // 创建临时节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hello/world3", "Hello World3".getBytes());
        // 创建临时有序节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/hello4/world4", "Hello World4".getBytes());

        client.close();
    }

    /**
     * 修改节点数据
     * @throws Exception
     */
    @Test
    public void updateNodeData() throws Exception {
        // 重试机制
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectStr = "node01.hadoop.com:2181,node02.hadoop.com:2181,node03.hadoop.com:2181";

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 1000, 1000, retryPolicy);

        // 创建连接
        client.start();

        client.setData().forPath("/hello/world1", "你好呀！！！".getBytes());

        client.close();
    }

    /**
     * 获取节点数据
     * @throws Exception
     */
    @Test
    public void getNodeData() throws Exception {
        // 重试机制
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectStr = "node01.hadoop.com:2181,node02.hadoop.com:2181,node03.hadoop.com:2181";

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 1000, 1000, retryPolicy);

        // 创建连接
        client.start();

        byte[] bytes = client.getData().forPath("/hello/world1");
        System.out.println(new String(bytes));

        client.close();
    }

    /**
     * 删除节点
     * @throws Exception
     */
    @Test
    public void deleteNode() throws Exception {
        // 重试机制
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectStr = "node01.hadoop.com:2181,node02.hadoop.com:2181,node03.hadoop.com:2181";

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 1000, 1000, retryPolicy);

        // 创建连接
        client.start();

        client.delete().forPath("/hello/world1");

        client.close();
    }

    /**
     * zk的watch机制
     * @throws Exception
     */
    @Test
    public void watchNode() throws Exception {
        // 重试机制
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        String connectStr = "node01.hadoop.com:2181,node02.hadoop.com:2181,node03.hadoop.com:2181";

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectStr, 1000, 1000, retryPolicy);

        // 创建连接
        client.start();
        // 监听指定路径
        TreeCache treeCache = new TreeCache(client, "/hello/world1");
        treeCache.getListenable().addListener((curatorFramework, treeCacheEvent) -> {
            ChildData data = treeCacheEvent.getData();
            if (null != data){
                // 不为空说明数据有变化

                // 获取节点数据变化的类型
                switch (treeCacheEvent.getType()) {
                    case NODE_ADDED:
                        System.out.println("NODE_ADDED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                        break;
                    case NODE_REMOVED:
                        System.out.println("NODE_REMOVED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                        break;
                    case NODE_UPDATED:
                        System.out.println("NODE_UPDATED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                        break;

                    default:
                        break;
                }
            }else{
                System.out.println( "data is null : "+ treeCacheEvent.getType());
            }
        });

        // 启动监听器
        treeCache.start();
        Thread.sleep(50000000);

        client.close();
    }
}
