package com.yi.demo7.fridens.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 好友列表数据，冒号前是一个用户，冒号后是该用户的所有好友（数据中的好友关系是单向的）
 * 求出哪些人两两之间有共同好友，及他俩的共同好友都有谁？
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class FridensStep2Main extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), FridensStep2Main.class.getName());
        job.setJarByClass(FridensStep2Main.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(FridensStep2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setReducerClass(FridensStep2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] array = new String[2];
        array[0] = "D:\\Temp\\step1";
        array[1] = "D:\\Temp\\step2";

        int run = ToolRunner.run(new Configuration(), new FridensStep2Main(), array);
        System.exit(run);
    }
}
