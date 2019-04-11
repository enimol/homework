package com.howo;

import com.github.javafaker.Faker;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HoWoMapper extends Mapper<LongWritable, Text, Text, Text> {

  private Integer count = 0;
  public static final String COUNT = "app.config.count";
  Faker faker = new Faker();

  @Override
  protected void setup(Context context) {
    this.count = Integer.parseInt(context.getConfiguration().get(COUNT));
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    Date now = new Date();
    Date past = null;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < this.count; i++) {
      sb.setLength(0);
      sb.append(faker.name().firstName() + ",");
      sb.append(faker.address().cityName() + ",");
      past = faker.date().past(1, TimeUnit.SECONDS, now);
      String lastName = faker.name().lastName();
      LocalDate ldate = LocalDate.from(past.toInstant().atZone(ZoneOffset.UTC));
      sb.append(DateTimeFormatter.ISO_DATE.format(ldate));
      context.write(new Text(lastName), new Text(sb.toString()));
    }
  }
}