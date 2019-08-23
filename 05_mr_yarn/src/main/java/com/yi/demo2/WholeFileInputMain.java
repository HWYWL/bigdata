package com.yi.demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 在业务处理之前，在HDFS上使用mapreduce程序对小文件进行合并
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class WholeFileInputMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(WholeFileInputMain.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        WholeFileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(WholeFileInputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 设置输出
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 没有record 但还是要设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] array = new String[2];
        array[0] = "D:\\gitCode\\bigdata\\05_mr_yarn\\src\\main\\resources\\merge";
        array[1] = "D:\\gitCode\\bigdata\\05_mr_yarn\\target\\out";

        int run = ToolRunner.run(new Configuration(), new WholeFileInputMain(), array);
        System.exit(run);
    }
}
