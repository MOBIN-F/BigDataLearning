//package com.mobin.Example;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
//
//
///**
// * Created by Mobin on 2016/12/22.
// */
//public class PutDataToHBase {
//    public static void main(String[] args) throws Exception {
//        Configuration conf = HBaseConfiguration.create();
//        LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
//        load.doBulkLoad(new Path("HFILE"), new HTable(conf,"STUDENT"));
//    }
//}
