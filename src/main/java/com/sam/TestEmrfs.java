package com.sam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by sam on 19/7/17.
 */
public class TestEmrfs {
    private static final Logger logger = LoggerFactory.getLogger(TestEmrfs.class);
    public static class Map extends Mapper<LongWritable, Text,LongWritable, Text> {
        @Override
        public void map(LongWritable key, Text logLine, Context context) throws IOException, InterruptedException {
            context.write(key,logLine);
        }
    }
    //reducer class
    public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
      @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          for(Text text : values){
              context.write(key,text);
          }
        }
    }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("starting test");
            Configuration conf = new Configuration();
            Job job = new Job(conf, "testemrfs");
            job.setJarByClass(TestEmrfs.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("s3://ebizu-kritter-campaign/postimpression/2017-07-19-04/"));
            FileOutputFormat.setOutputPath(job, new Path("/outputs3"));
            boolean result = job.waitForCompletion(true);
            System.out.println("Job completed successfully. Check your expected output @ hdfs://outputs3/");
            System.exit(result ? 0 : 1);
        }
}
