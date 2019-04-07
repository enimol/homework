package com.howo;

import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class HoWoMapper extends Mapper<LongWritable, Text, Text, Text>{
 
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		context.write(new Text("bubu") , new Text("fifi") );
	}
}