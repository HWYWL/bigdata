package com.yi.demo3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 将订单的好评与差评进行区分开来，将最终的数据分开到不同的文件夹下面去
 *
 * @author huangwenyi
 * @date 2019-8-23
 */
public class MyOutputMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(MyOutputMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(MyOutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置输出
        job.setOutputFormatClass(MyOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 没有record 但还是要设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] array = new String[2];
        array[0] = "D:\\gitCode\\bigdata\\05_mr_yarn\\src\\main\\resources\\comment";
        array[1] = "D:\\gitCode\\bigdata\\05_mr_yarn\\target\\out";

        int run = ToolRunner.run(new Configuration(), new MyOutputMain(), array);
        System.exit(run);
    }
}
