package com.yi.demo5.flow.partition;

import com.yi.demo5.flow.partition.entity.PhonePartitionBean;
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
 * 对不同地区的手机号码进行分区 写入不同的分区文件 需要部署到集群 不然报错
 * 135 开头数据到一个分区文件
 * 136 开头数据到一个分区文件
 * 137 开头数据到一个分区文件
 * 138 开头数据到一个分区文件
 * 139 开头数据到一个分区文件
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class PhonePartitionMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), PhonePartitionMain.class.getName());
        job.setJarByClass(PhonePartitionMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(PhonePartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhonePartitionBean.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setReducerClass(PhonePartitionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhonePartitionBean.class);

        // 设置分区
        job.setPartitionerClass(PhonePartition.class);
        // 设置分区线程 必须和分区大小一致 不然会产生多余的空白文件
        job.setNumReduceTasks(6);

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] array = new String[2];
        array[0] = "/test/flow";
        array[1] = "/test/flowout";
        int run = ToolRunner.run(new Configuration(), new PhonePartitionMain(), array);
        System.exit(run);
    }
}
