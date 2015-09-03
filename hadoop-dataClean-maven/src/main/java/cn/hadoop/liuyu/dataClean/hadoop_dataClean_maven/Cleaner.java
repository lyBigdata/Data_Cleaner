package cn.hadoop.liuyu.dataClean.hadoop_dataClean_maven;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @function  解析日志数据
 * @author liuyu
 *
 */
class LogParse{
	public static final SimpleDateFormat FORMAT=new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
	public static final SimpleDateFormat dateformat=new SimpleDateFormat("yyyyMMddHHmmss");
	
	/**
	 * 解析日志文件的行记录数据
	 * @param line
	 * @return
	 */
	public String[] parse(String line){
		String ip=parseIp(line);  
		
		String time;
		try{
			time=parseTime(line);
		}catch(Exception e0){
			time="null";
		}
		
		String url;
		try{
			url=parseURL(line);
		}catch(Exception e1){
			url="null";
		}
		
		String status=parseStatus(line);
		
		String traffic=parseTraffic(line);
		
		return new String[]{ip,time,url,status,traffic};
	}
	
	private String parseTraffic(String line){
		String sub ;
		try{
			sub=line.substring(line.lastIndexOf("\"")+1);
			String traffic = sub.split(" ")[1];
			return traffic;
		}catch(Exception e){
			sub="null";
		}
		
		return sub;
	}
	
	private String parseStatus(String line){
		String sub;
		try{
			sub=line.substring(line.lastIndexOf("\"")+1);
		}catch(Exception e){
			sub="null";
		}
		
		String status=sub.split(" ")[0];
		
		return status;
	}
	
	private String parseURL(String line){
		final int first =line.indexOf("\"");
		final int last=line.lastIndexOf("\"");
		String url=line.substring(first+1,last);
		
		return url;
	}
	private String parseTime(String line){
		final int first=line.indexOf("[");
		final int last=line.lastIndexOf("+0800]");
		
		String time=line.substring(first+1,last).trim();
		try{
			return dateformat.format(FORMAT.parse(time));
		}catch(ParseException e){
			e.printStackTrace();
		}
		return "";
	}
	
	private String   parseIp(String line){
		String ip = line.split("- -")[0].trim();
		return ip;
	}
}

public class Cleaner  extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, Cleaner.class.getSimpleName());
		
		job.setJarByClass(Cleaner.class);
		
		final String input=args[0];
		final String output=args[1];
		
		Path outputpath=new Path(output);
		FileSystem fs=outputpath.getFileSystem(conf);
		if(fs.isDirectory(outputpath)){
			fs.delete(outputpath, true);
		}
		
		FileInputFormat.setInputPaths(job, input); 
		
		job.setMapperClass(CleanMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(CleanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job, outputpath);
		
		job.waitForCompletion(true);
		
		return 0;
	}

	public static class CleanMapper extends Mapper<LongWritable,Text,Text,Text>{

		LogParse parser = new LogParse();
		Text outkey=new Text();
		Text outvalue=new Text();
			
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			final String line=value.toString();
			final String[] parsed=parser.parse(line);
			
			final String ip=parsed[0];
			final String time=parsed[1];
			String url=parsed[2];
			final String status=parsed[3];
			final String  traffic=parsed[4];
			
			/**
			 * 27.17.29.35   XXXXXX   GET /static/js/md5.js?y7a HTTP/1.1   200  5734
			 */
			//过滤所有的静态资源请求
			if(url.startsWith("GET /static")||url.startsWith("GET /uc_server")){
				return;
			}
			
			
			//从url中提取客户请求的资源
			if(url.startsWith("GET")){
				url=url.substring("GET".length()+1, url.length()-"HTTP/1.1".length()).trim();
			}
			if(url.startsWith("POST")){
				url = url.substring("POST ".length()+1, url.length()-" HTTP/1.1".length()).trim();
			}
			
			outvalue.set(time+"\t"+url+"\t"+status+"\t"+traffic);
			outkey.set(ip);
			
			context.write(outkey, outvalue);
			}
	}
	
	public static class CleanReducer extends Reducer<Text,Text,Text,NullWritable>{

		Text outkey=new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			for(Text value:values){
				outkey.set(key.toString()+"\t'"+value.toString());
				context.write(outkey, NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Cleaner(), args);
	}
}
