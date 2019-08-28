package com.yi.weblog.clickstream;

import com.yi.weblog.mrbean.PageViewsBean;
import com.yi.weblog.mrbean.VisitBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;


/**
 * 输入数据：pageviews模型结果数据
 * 从pageviews模型结果数据中进一步梳理出visit模型
 * sessionid  start-time   out-time   start-page   out-page   pagecounts  ......
 * 
 * @author
 *
 */
public class ClickStreamVisit extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf);

	/*	String inputPath = "hdfs://node01:8020/weblog/"+ DateUtil.getYestDate() + "/pageViewOut";
		String outPutPath="hdfs://node01:8020/weblog/"+ DateUtil.getYestDate() + "/clickStreamVisit";
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"),conf);
		if (fileSystem.exists(new Path(outPutPath))){
			fileSystem.delete(new Path(outPutPath),true);
		}
		fileSystem.close();
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outPutPath));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);*/


		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job,new Path("D:\\gitCode\\bigdata\\08_web_log_parser\\src\\main\\resources\\pageViewOut2"));
		TextOutputFormat.setOutputPath(job,new Path("D:\\gitCode\\bigdata\\08_web_log_parser\\target\\clickStreamVisit2"));

		job.setJarByClass(ClickStreamVisit.class);
		job.setMapperClass(ClickStreamVisitMapper.class);
		job.setReducerClass(ClickStreamVisitReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageViewsBean.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VisitBean.class);
		boolean res = job.waitForCompletion(true);
		return res?0:1;
	}

	// 以session作为key，发送数据到reducer
	/**
	 * 这里的key2   用session来作为我们的key2
	 */
	static class ClickStreamVisitMapper extends Mapper<LongWritable, Text, Text, PageViewsBean> {
		PageViewsBean pvBean = new PageViewsBean();
		Text k = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\001");
			int step = Integer.parseInt(fields[5]);
			//(String session, String remote_addr, String timestr, String request, int step, String staylong, String referal, String useragent, String bytes_send, String status)
			//299d6b78-9571-4fa9-bcc2-f2567c46df3472.46.128.140-2013-09-18 07:58:50/hadoop-zookeeper-intro/160"https://www.google.com/""Mozilla/5.0"14722200
			pvBean.set(fields[0], fields[1], fields[2], fields[3],fields[4], step, fields[6], fields[7], fields[8], fields[9]);
			//以我们的session来作为我们的key2，相同session的页面访问记录都会到同一个reduce里面去，形成一个集合
			k.set(pvBean.getSession());
			context.write(k, pvBean);
		}
	}

	static class ClickStreamVisitReducer extends Reducer<Text, PageViewsBean, NullWritable, VisitBean> {

		@Override
		protected void reduce(Text session, Iterable<PageViewsBean> pvBeans, Context context) throws IOException, InterruptedException {

			// 将pvBeans按照step排序
			ArrayList<PageViewsBean> pvBeansList = new ArrayList<PageViewsBean>();
			for (PageViewsBean pvBean : pvBeans) {
				PageViewsBean bean = new PageViewsBean();
				try {
					BeanUtils.copyProperties(bean, pvBean);
					pvBeansList.add(bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			/**
			 * 将数据按照我们的步骤进行排序，这样就可以得到哪个页面先访问，哪个页面后访问的
			 */
			Collections.sort(pvBeansList, new Comparator<PageViewsBean>() {
				@Override
				public int compare(PageViewsBean o1, PageViewsBean o2) {
					return o1.getStep() > o2.getStep() ? 1 : -1;
				}
			});

			// 取这次visit的首尾pageview记录，将数据放入VisitBean中
			VisitBean visitBean = new VisitBean();
			// 取visit的首记录
			visitBean.setInPage(pvBeansList.get(0).getRequest());
			visitBean.setInTime(pvBeansList.get(0).getTimestr());
			// 取visit集合当中末尾的记录即可
			visitBean.setOutPage(pvBeansList.get(pvBeansList.size() - 1).getRequest());
			visitBean.setOutTime(pvBeansList.get(pvBeansList.size() - 1).getTimestr());
			// visit访问的页面数
			visitBean.setPageVisits(pvBeansList.size());
			// 来访者的ip
			visitBean.setRemote_addr(pvBeansList.get(0).getRemote_addr());
			// 本次visit的referal
			visitBean.setReferal(pvBeansList.get(0).getReferal());
			visitBean.setSession(session.toString());
			context.write(NullWritable.get(), visitBean);
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(),new ClickStreamVisit(),args);
	}

}
