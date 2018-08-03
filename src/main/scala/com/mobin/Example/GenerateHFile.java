//package com.mobin.Example;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//import java.io.IOException;
//
///**
// * Created by Mobin on 2016/12/22.
// */
//public class GenerateHFile {
//
//    static class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String[] line = value.toString().split(",");
//            String rk = line[0];
//            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(rk.getBytes());
//            Put put = new Put(rk.getBytes());
//            put.addColumn("S".getBytes(),"name".getBytes(), line[1].getBytes());
//            put.addColumn("S".getBytes(), "sex".getBytes(), line[2].getBytes());
//            put.addColumn("S".getBytes(), "age".getBytes(), line[3].getBytes());
//            put.addColumn("S".getBytes(), "class".getBytes(), line[4].getBytes());
//            context.write(rowkey, put);
//        }
//    }
//
//    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        final String INPUT_PATH = "/DATA/PUBLIC/NOCE/SGC/Student.txt";
//        final String OUT_PATH = "/DATA/PUBLIC/NOCE/SGC/HFILE";
//        Configuration conf = HBaseConfiguration.create();
//        HTable table = new HTable(conf,"STUDENT");
//        Job job = Job.getInstance(conf);
//        job.setJarByClass(GenerateHFile.class);
//        job.setMapperClass(HFileMapper.class);
//        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//        job.setMapOutputValueClass(Put.class);
//
//        job.setOutputFormatClass(HFileOutputFormat2.class);
//        HFileOutputFormat2.configureIncrementalLoad(job,table,table.getRegionLocator());
//        FileInputFormat.setInputPaths(job, INPUT_PATH);
//        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
//        System.exit(job.waitForCompletion(true)?0:1);
//
//    }
//}
