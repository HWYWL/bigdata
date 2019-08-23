package com.yi.demo4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用分组求出每一个订单中成交金额最大的一笔交易
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class OrderMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), OrderMain.class.getName());
        //打包到线上运行，需要这一句
        job.setJarByClass(OrderMain.class);

        //第一步：读取文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        //第二步：自定义map逻辑
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //第三步：分区  排序
        job.setPartitionerClass(OrderIdPartition.class);

        //第六步：分组
        job.setGroupingComparatorClass(OrderIdWritableComparator.class);

        //第七步：reduce阶段
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //第八步：输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] array = new String[2];
        array[0] = "D:\\gitCode\\bigdata\\05_mr_yarn\\src\\main\\resources\\grouping";
        array[1] = "D:\\gitCode\\bigdata\\05_mr_yarn\\target\\out";

        int run = ToolRunner.run(new Configuration(), new OrderMain(), array);
        System.exit(run);
    }

}
