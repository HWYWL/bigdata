package com.yi.demo1;

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
 * 统计文本在文件中出现的次数
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class IndexMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), IndexMain.class.getName());
        job.setJarByClass(IndexMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] array = new String[2];
        array[0] = "D:\\gitCode\\bigdata\\05_mr_yarn\\src\\main\\resources\\index";
        array[1] = "D:\\gitCode\\bigdata\\05_mr_yarn\\target\\out";

        int run = ToolRunner.run(new Configuration(), new IndexMain(), array);
        System.exit(run);
    }
}
