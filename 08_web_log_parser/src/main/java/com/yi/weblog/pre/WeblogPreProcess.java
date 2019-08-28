package com.yi.weblog.pre;

import com.yi.weblog.mrbean.WebLogBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 处理原始日志，过滤出真实pv请求 转换时间格式 对缺失字段填充默认值 对记录标记valid和invalid
 * hadoop jar weblog.jar  com.yi.weblog.pre.WeblogPreProcess /weblog/input/weblog/preout
 */

public class WeblogPreProcess extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		//Configuration conf = new Configuration();
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf);



	/*	String inputPath= "hdfs://node01:8020/weblog/"+DateUtil.getYestDate()+"/input";
		String outputPath="hdfs://node01:8020/weblog/"+DateUtil.getYestDate()+"/weblogPreOut";
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), conf);
		if (fileSystem.exists(new Path(outputPath))){
			fileSystem.delete(new Path(outputPath),true);
		}
		fileSystem.close();
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);*/

		FileInputFormat.addInputPath(job,new Path("D:\\gitCode\\bigdata\\08_web_log_parser\\src\\main\\resources\\input"));
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job,new Path("D:\\gitCode\\bigdata\\08_web_log_parser\\target\\weblogPreOut2"));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setJarByClass(WeblogPreProcess.class);

		//如果要打包到集群上面去运行，这一句一定要添加上，不然报错不好使
		job.setJarByClass(WeblogPreProcess.class);
		job.setMapperClass(WeblogPreProcessMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		boolean res = job.waitForCompletion(true);
		return res?0:1;
	}

	/**
	 * 如果mapper和reducer都写成内部类，那么就必须加上static关键字
	 */
	static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		// 用来存储网站url分类数据
		Set<String> pages = new HashSet<String>();
		Text k = new Text();
		NullWritable v = NullWritable.get();
		/**
		 * map阶段的初始化方法
		 * 从外部配置文件中加载网站的有用url分类数据 存储到maptask的内存中，用来对日志数据进行过滤
		 * 过滤掉我们日志文件当中的一些静态资源，包括js   css  img  等请求日志都需要过滤掉
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//定义一个集合，集合当中过滤掉我们的一些静态资源
			pages.add("/about");
			pages.add("/black-ip-list/");
			pages.add("/cassandra-clustor/");
			pages.add("/finance-rhive-repurchase/");
			pages.add("/hadoop-family-roadmap/");
			pages.add("/hadoop-hive-intro/");
			pages.add("/hadoop-zookeeper-intro/");
			pages.add("/hadoop-mahout-roadmap/");

		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//得到我们一行数据
			String line = value.toString();
			WebLogBean webLogBean = WebLogParser.parser(line);
			if (webLogBean != null) {
				// 过滤js/图片/css等静态资源
				WebLogParser.filtStaticResource(webLogBean, pages);
				/* if (!webLogBean.isValid()) return; */
				k.set(webLogBean.toString());
				context.write(k, v);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		int run = ToolRunner.run(configuration, new WeblogPreProcess(), args);
		System.exit(run);
	}

}
