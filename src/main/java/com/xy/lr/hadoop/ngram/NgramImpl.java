package com.xy.lr.hadoop.ngram;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by ruili on 2015/12/6.
 * N-gram Main
 */
public class NgramImpl {
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 3){
			System.err.println("Usage: NgramImpl <in> <out> <N-gram这个只支持2元和3元>");
			System.exit(2);
		}
		Job job = new Job(conf, "NgramImpl");
		
		//设置执行的类包括主类、Mapper类、Reducer类
		job.setJarByClass(NgramImpl.class);
		String ngram = otherArgs[2];
		if(ngram.equals("2")){
			job.setMapperClass(N2gramMapperImpl.class);
		}else if(ngram.equals("3")){
			job.setMapperClass(N3gramMapperImpl.class);
		}else{
			System.err.println("当前只支持2元和3元的gram！！！");
			System.exit(2);
		}
		job.setReducerClass(NgramReducerImpl.class);
		
		//设置中间输出key和value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit( job.waitForCompletion(true) ? 0 : 1 );
	}
}