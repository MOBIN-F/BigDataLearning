package com.mobin.Example;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Mobin on 2016/12/22.
 * 性别统计
 */
public class SexCount {
    static class SexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            context.write(new Text(line[2]), new IntWritable(1));
        }
    }

    static class SexReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable c : values)
                count += c.get();
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final  String INPUT_PATH = "Student.txt";
        final  String OUT_PATH = "StudentSex";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(SexMapper.class);
        job.setReducerClass(SexReduce.class);
        job.setJarByClass(SexCount.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, INPUT_PATH);
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
        System.exit(job.waitForCompletion(true)?0:1);

    }
}
