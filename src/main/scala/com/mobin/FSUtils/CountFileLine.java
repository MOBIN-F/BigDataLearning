package com.mobin.FSUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import sun.applet.Main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * Created by Mobin on 2016/12/20.
 * 统计一个目录下的lzo文件的行数，每个lzo起一个task
 */
public class CountFileLine implements Callable<Integer>{
    public FileSystem fs;
    public String path;

    @Override
    public Integer call() throws Exception {
        return  countLine(fs,path);
    }

    public Integer countLine(FileSystem fs,String path) throws IOException {
        int count = 0;
        FSUtils.BufferedReadIterable brl = new FSUtils.BufferedReadIterable(fs,path);
        for(String line: brl){
            count ++;
        }
        System.out.println(count);
        return count;
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        int sum=0;
        String file = "E:\\DATA\\PUBLIC\\NOCE\\AGG\\AGG_EVT_LTE_DPI_NEW\\hour=2016102011";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        ArrayList<Future<Integer>> tasks = new ArrayList<>();
        File[] files = new File(file).listFiles();
        for(File f: files){
            if(f.getName().endsWith(".lzo")){
                CountFileLine cd = new CountFileLine();
                cd.fs = fs;
                cd.path = f.getPath();
                FutureTask<Integer> task = new FutureTask<Integer>(cd);
                tasks.add(task);
                Thread thread = new Thread(task);
                System.out.println(thread.getName());
                thread.start();
            }
        }

        for(Future<Integer> future: tasks){
            sum += future.get();
        }
        System.out.println(sum);

    }
}
