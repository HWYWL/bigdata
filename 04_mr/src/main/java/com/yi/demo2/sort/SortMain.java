package com.yi.demo2.sort;

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
 * 排序
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class SortMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), SortMain.class.getName());
        job.setJarByClass(SortMain.class);

        // 读取文件解析成key value对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        // 设置mapper拆分服务
        job.setMapperClass(SortMapper.class);
        // 设置k2 v2的输出类型
        job.setMapOutputKeyClass(PairSort.class);
        job.setMapOutputValueClass(Text.class);

        // 设置规约
//        job.setCombinerClass(MyCombiner.class);

        // 设置结果集合并
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(PairSort.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置数据输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] array = new String[2];
        array[0] = "D:\\gitCode\\bigdata\\04_mr\\src\\main\\resources\\sort";
        array[1] = "D:\\gitCode\\bigdata\\04_mr\\target\\out";
        int run = ToolRunner.run(new Configuration(), new SortMain(), array);
        System.exit(run);
    }
}
