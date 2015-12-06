package com.xy.lr.hadoop.ngram;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by ruili on 2015/12/6.
 * N-gram Reducer
 */
public class NgramReducerImpl 
	extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException{
		int sum = 0;
		
		//累加value的值
		for(IntWritable value : values){
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
}