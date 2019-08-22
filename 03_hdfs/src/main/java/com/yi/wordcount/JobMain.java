package com.yi.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 任务
 *
 * @author huangwenyi
 * @date 2019-8-21
 */
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 集群模式 数据来源，wordcount目录下的文件
        String inPath = "hdfs://node01.hadoop.com:8020/wordcount";
        // 集群模式 数据输出的位置
        String outPath = "hdfs://node01.hadoop.com:8020/wordcountout";

        // 本地调试模式 数据来源，wordcount目录下的文件
//        String inPath = "file:///D:\\gitCode\\bigdata\\03_hdfs\\src\\test\\resources\\input";
        // 本地模式 数据输出的位置
//        String outPath = "file:///D:\\gitCode\\bigdata\\03_hdfs\\target\\output";

        Job job = Job.getInstance(super.getConf(), "Job-1");

        // 如果打成jar包运行 必须加上这句
        job.setJarByClass(JobMain.class);

        // 读取输入文件解析成key，value对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inPath));

        // 自定义map逻辑，接收第一步的k1,v1  转换成新的k2  v2  进行输出
        job.setMapperClass(WordCountMapper.class);
        // 设置我们key2的类型
        job.setMapOutputKeyClass(Text.class);
        // 设置我们的v2类型
        job.setMapOutputValueClass(IntWritable.class);

        // 分区  相同key的value发送到同一个reduce里面去，形成一个集合
        // 排序
        // 规约
        // 分组
        // 以上操作可以省略

        // 设置我们的reduce类，接受我们的key2  v2  输出我们k3  v3
        job.setReducerClass(WordCountReducer.class);
        // 设置key3输出的类型
        job.setOutputKeyClass(Text.class);
        // 设置value3输出的类型
        job.setOutputValueClass(IntWritable.class);

        // 设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);

        // 输出处理完的结果
        TextOutputFormat.setOutputPath(job, new Path(outPath));

        // 提交任务
        boolean wait = job.waitForCompletion(true);

        return wait ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 完成任务，返回状态码，0：运行成功
        int run = ToolRunner.run(new Configuration(), new JobMain(), args);
        System.exit(run);
    }
}
