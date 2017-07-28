package com.mobin.HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Mobin on 2017/2/4.
 */
public class LzoCompress {
    public static void main(String[] args)  {
        Configuration conf = new Configuration();
        try {
         FileSystem fs = FileSystem.get(conf);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }



//    public void LzoCoder(){
//       try(){
//
//        }
//    }
}
