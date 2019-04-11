package com.howo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Apa
 *
 */
public class HomeWork extends Configured implements Tool {

  /**
   * My homework.
   * @param args args[0]: count, args[1]: output folder.
   * @throws Exception Exception
   */
  public static void main(final String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HomeWork(), args);
    System.exit(exitCode);
  }

  /**
   * Here we go...
   * @param args args[0]: HomeWork, args[1]: main args.
   * @throws Exception Exception
   * @return -1
   */
  public final int run(final String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <count> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    Job job = new org.apache.hadoop.mapreduce.Job();

    Configuration configuration = job.getConfiguration();
    configuration.set("mapred.textoutputformat.separator", ",");
    configuration.set("fs.defaultFS",
        "hdfs://sandbox-hdp.hortonworks.com:8020");
    configuration.set(HoWoMapper.COUNT, args[0]);

    FileSystem fs = FileSystem.get(configuration);
    FSDataOutputStream stream = fs.create(new Path("/tmp/tmp"));
    stream.write(args[0].getBytes());
    stream.flush();
    stream.close();

    FileInputFormat.addInputPath(job, new Path("/tmp/tmp"));

    job.setJarByClass(HomeWork.class);
    job.setJobName("HomeWork");

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(HoWoMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    int returnValue = 0;
    if (job.waitForCompletion(true)) {
      returnValue = 0;
    } else {
      returnValue = 1;
    }
    System.out.println("job.isSuccessful " + job.isSuccessful());
    return returnValue;
  }
}
