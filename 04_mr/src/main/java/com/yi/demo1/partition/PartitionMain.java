package com.yi.demo1.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 数据分区 需要部署到集群运行
 *
 * @author huangwenyi
 * @date 2019-8-21
 */
public class PartitionMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 获取job 用来组装MR任务
        Job job = Job.getInstance(super.getConf(), PartitionMain.class.getName());

        // 打包运行必要
        job.setJarByClass(PartitionMain.class);

        // 读取文件解析类，解析成key,value模型
        job.setInputFormatClass(TextInputFormat.class);
        // 分区 不能在本地执行 可以打包成jar放到集群中执行
        TextInputFormat.addInputPath(job, new Path(args[0]));

        // 自定义map逻辑
        job.setMapperClass(PartitionMapper.class);
        // 设置k2 v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置分区类，让数据按照我们自定义的方式进行分区
        job.setPartitionerClass(PartitionerOwn.class);

        //排序、规约、分组 忽略

        // 自定义reduce聚合逻辑
        job.setReducerClass(PartitionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reduceTask数量, reduceTask的个数一定要与分区数保持一致
        job.setNumReduceTasks(2);

        // 设置输出类
        job.setOutputFormatClass(TextOutputFormat.class);

        // 通过参数获取输出路径
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] array = new String[2];
        // 输入数据源文件夹
        array[0] = "/test/inpartition";
        // 输出分类好的数据
        array[1] = "/test/outpartition";

        int run = ToolRunner.run(new Configuration(), new PartitionMain(), array);
        System.exit(run);
    }
}
