package com.xy.lr.hadoop.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ruili on 2015/12/6.
 * 3-gram Mapper
 */
public class N3gramMapperImpl 
	extends Mapper<LongWritable, Text, Text, IntWritable>{
  //常量值1
  private final IntWritable one = new IntWritable(1);

  @Override
  public void map (LongWritable key, Text value, Context context)
      throws IOException, InterruptedException{
    //处理每一行的数据
    String eachLine = value.toString();

    //错误行判断
    if(eachLine.split(" ").length <= 1) {
      return ;
    }
    int number = 0;
    //以空格分割输入数据
    String[] splitLineArray = eachLine.split(" ");

    for ( int num = 0 ; num < splitLineArray.length - 2 ; num++ ) {
    	String wi = splitLineArray[num];
    	String wi1 = splitLineArray[num + 1];
    	Text mapOutputKey = new Text(wi + "_" + wi1);
    	context.write(mapOutputKey, one);
    }
    
    for ( int num = 0 ; num < splitLineArray.length ; num++ , number++ ) {
    	if(number < 2){}
    	else{
    		String wi_2 = splitLineArray[num - 2];
    		String wi_1 = splitLineArray[num - 1];
    		String wi = splitLineArray[num];
    		Text mapOutputKey = new Text(wi_2 + "_" +wi_1 + "@" + wi);
    		context.write(mapOutputKey, one);
    	}
    }
  }
}
