package com.howo;

import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.github.javafaker.Faker;
 
public class HoWoMapper extends Mapper<LongWritable, Text, Text, Text>{
 
	private final static IntWritable one = new IntWritable(1);
    private Integer count = 0;
    public static final String COUNT = "app.config.count";
    Faker faker = new Faker();
    
    @Override
    protected void setup(Context context) {
       this.count = Integer.parseInt(context.getConfiguration().get(COUNT));
    }

    @Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
    	 for(int i=0; i < this.count; i++) {
				String lastName = faker.name().fullName();
				String firstName = faker.name().firstName();
    			context.write(new Text(lastName) , new Text(firstName) );
    	 }
	}
}