package com.yi.demo4.flow.sort;

import com.yi.demo4.flow.sort.entity.FlowSortBean;
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
 * 对所有数据中下行流量进行倒叙排序
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class FlowSortCountMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(FlowSortCountMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(FlowSortMapper.class);
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setReducerClass(FlowSortReducer.class);
        job.setOutputKeyClass(FlowSortBean.class);
        job.setOutputValueClass(Text.class);

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] array = new String[2];
        array[0] = "D:\\gitCode\\bigdata\\04_mr\\src\\main\\resources\\flow";
        array[1] = "D:\\gitCode\\bigdata\\04_mr\\target\\out";

        int run = ToolRunner.run(new Configuration(), new FlowSortCountMain(), array);
        System.exit(run);
    }
}
