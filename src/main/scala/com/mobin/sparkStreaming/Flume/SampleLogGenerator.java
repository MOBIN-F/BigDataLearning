package com.mobin.sparkStreaming.Flume;

import java.io.*;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/2
 * Time: 4:39 PM
 */
public class SampleLogGenerator {
    public static void main(String[] args) throws IOException, InterruptedException {
            String location = "/Users/mobin/Downloads/access_log/access1_log";
            File f = new File(location);
            FileOutputStream writer = new FileOutputStream(f);
            File read = new File("/Users/mobin/Downloads/access_log/access_log");
            BufferedReader reader = new BufferedReader(new FileReader(read));
            for(;;){
                System.out.println("....");
                writer.write((reader.readLine() + "\n").getBytes());
                writer.flush();
                Thread.sleep(500);
            }
    }
}
