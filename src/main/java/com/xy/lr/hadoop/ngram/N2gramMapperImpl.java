package com.xy.lr.hadoop.ngram;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by ruili on 2015/12/6.
 * 2-gram Mapper
 */
public class N2gramMapperImpl 
	extends Mapper<LongWritable, Text, Text, IntWritable>{
	//常量值1
	private final IntWritable one = new IntWritable(1);
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String eachLine = value.toString();
		//错误行判断
		if(eachLine.split(" ").length <= 1){
			return;
		}
		//第一个字符的标识
		boolean flag = true;
		//以空格分割输入数据
		String[] splitLineArray = eachLine.split(" ");
		//遍历
		for( int i = 0 ; i < splitLineArray.length ; i++ ){
			if(flag){
				flag = false;
			}else{//输出相邻的两个词出现的次数
				String wi_1 = splitLineArray[i - 1];
				String wi = splitLineArray[i];
				Text mapOutputKey = new Text(wi_1 + "@" + wi);
				context.write(mapOutputKey, one);
			}
			//输出每个词的出现次数
			String wi = splitLineArray[i];
			Text mapOutPutKey = new Text(wi);
			context.write(mapOutPutKey, one);
		}
	}
}